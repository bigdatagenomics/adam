/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.variant

import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.models.{
  Coverage,
  ReferenceRegion,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.{ CoverageRDD, FeatureRDD }
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.sql.{
  AlignmentRecord => AlignmentRecordProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  NucleotideContigFragment => NucleotideContigFragmentProduct,
  Variant => VariantProduct
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

object VariantRDDSuite extends Serializable {

  def covFn(v: Variant): Coverage = {
    Coverage(v.getContigName,
      v.getStart,
      v.getEnd,
      1)
  }

  def covFn(vc: VariantContext): Coverage = {
    covFn(vc.variant.variant)
  }

  def featFn(v: Variant): Feature = {
    Feature.newBuilder
      .setContigName(v.getContigName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build
  }

  def featFn(vc: VariantContext): Feature = {
    featFn(vc.variant.variant)
  }

  def fragFn(v: Variant): Fragment = {
    Fragment.newBuilder
      .setReadName(v.getContigName)
      .build
  }

  def fragFn(vc: VariantContext): Fragment = {
    fragFn(vc.variant.variant)
  }

  def ncfFn(v: Variant): NucleotideContigFragment = {
    NucleotideContigFragment.newBuilder
      .setContigName(v.getContigName)
      .build
  }

  def ncfFn(vc: VariantContext): NucleotideContigFragment = {
    ncfFn(vc.variant.variant)
  }

  def readFn(v: Variant): AlignmentRecord = {
    AlignmentRecord.newBuilder
      .setContigName(v.getContigName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build
  }

  def readFn(vc: VariantContext): AlignmentRecord = {
    readFn(vc.variant.variant)
  }

  def genFn(v: Variant): Genotype = {
    Genotype.newBuilder
      .setContigName(v.getContigName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build
  }

  def genFn(vc: VariantContext): Genotype = {
    genFn(vc.variant.variant)
  }

  def vcFn(v: Variant): VariantContext = {
    VariantContext(Variant.newBuilder
      .setContigName(v.getContigName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build)
  }
}

class VariantRDDSuite extends ADAMFunSuite {

  sparkTest("union two variant rdds together") {
    val variant1 = sc.loadVariants(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
    val variant2 = sc.loadVariants(testFile("small.vcf"))
    val union = variant1.union(variant2)
    assert(union.rdd.count === (variant1.rdd.count + variant2.rdd.count))
    assert(union.sequences.size === (variant1.sequences.size + variant2.sequences.size))
  }

  sparkTest("round trip to parquet") {
    val variants = sc.loadVariants(testFile("small.vcf"))
    val outputPath = tmpLocation()
    variants.saveAsParquet(outputPath)
    val unfilteredVariants = sc.loadVariants(outputPath)
    assert(unfilteredVariants.rdd.count === 6)

    val predicate = ReferenceRegion.toEnd("1", 752720L).toPredicate
    val filteredVariants = sc.loadParquetVariants(outputPath,
      optPredicate = Some(predicate))
    assert(filteredVariants.rdd.count === 2)
    val starts = filteredVariants.rdd.map(_.getStart).distinct.collect.toSet
    assert(starts.size === 2)
    assert(starts(752720L))
    assert(starts(752790L))
  }

  sparkTest("use broadcast join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = variants.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 3L)
  }

  sparkTest("use right outer broadcast join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = variants.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 3)
  }

  sparkTest("use shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.shuffleRegionJoin(targets)
    val jRdd0 = variants.shuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 3L)
    assert(jRdd0.rdd.count === 3L)

    val joinedVariants: VariantRDD = jRdd
      .transmute((rdd: RDD[(Variant, Feature)]) => {
        rdd.map(_._1)
      })
    val tempPath = tmpLocation(".adam")
    joinedVariants.saveAsParquet(tempPath)
    assert(sc.loadVariants(tempPath).rdd.count === 3)
  }

  sparkTest("use right outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 3)
    assert(c0.count(_._1.isDefined) === 3)
  }

  sparkTest("use left outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 3)
    assert(c0.count(_._2.isEmpty) === 3)
    assert(c.count(_._2.isDefined) === 3)
    assert(c0.count(_._2.isDefined) === 3)
  }

  sparkTest("use full outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 3)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 3)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 3)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 3)
  }

  sparkTest("use shuffle join with group by to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = variants.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 3)
    assert(c0.size === 3)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = variants.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect

    assert(c.count(_._1.isDefined) === 6)
    assert(c0.count(_._1.isDefined) === 6)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 3)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 3)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 3)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 3)
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  sparkTest("convert back to variant contexts") {
    val variantsPath = testFile("small.vcf")
    val variants = sc.loadVariants(variantsPath)
    val variantContexts = variants.toVariantContexts

    assert(variantContexts.sequences.containsRefName("1"))
    assert(variantContexts.samples.isEmpty)

    val vcs = variantContexts.rdd.collect
    assert(vcs.size === 6)

    val vc = vcs.head
    assert(vc.position.referenceName === "1")
    assert(vc.variant.variant.contigName === "1")
    assert(vc.genotypes.isEmpty)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    def testMetadata(vRdd: VariantRDD) {
      val sequenceRdd = vRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsRefName("aSequence"))

      val headerRdd = vRdd.addHeaderLine(new VCFHeaderLine("ABC", "123"))
      assert(headerRdd.headerLines.exists(_.getKey == "ABC"))
    }

    val inputPath = testFile("small.vcf")
    val outputPath = tmpLocation()
    val rrdd = sc.loadVariants(inputPath)
    testMetadata(rrdd)
    val rdd = rrdd.transformDataset(ds => ds) // no-op but force to sql
    testMetadata(rdd)
    assert(rdd.dataset.count === 6)
    assert(rdd.rdd.count === 6)
    rdd.saveAsParquet(outputPath)
    println(outputPath)
    val rdd2 = sc.loadVariants(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 6)
    assert(rdd2.dataset.count === 6)
    val outputPath2 = tmpLocation()
    println(outputPath2)
    rdd.transform(rdd => rdd) // no-op but force to rdd
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadVariants(outputPath2)
    assert(rdd3.rdd.count === 6)
    assert(rdd3.dataset.count === 6)
  }

  sparkTest("transform variants to contig rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(contigs: NucleotideContigFragmentRDD) {
      val tempPath = tmpLocation(".adam")
      contigs.saveAsParquet(tempPath)

      assert(sc.loadContigFragments(tempPath).rdd.count === 6)
    }

    val contigs: NucleotideContigFragmentRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.ncfFn)
    })

    checkSave(contigs)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val contigsDs: NucleotideContigFragmentRDD = variants.transmuteDataset(ds => {
      ds.map(r => {
        NucleotideContigFragmentProduct.fromAvro(
          VariantRDDSuite.ncfFn(r.toAvro))
      })
    })

    checkSave(contigsDs)
  }

  sparkTest("transform variants to coverage rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(coverage: CoverageRDD) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 6)
    }

    val coverage: CoverageRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.covFn)
    })

    checkSave(coverage)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val coverageDs: CoverageRDD = variants.transmuteDataset(ds => {
      ds.map(r => VariantRDDSuite.covFn(r.toAvro))
    })

    checkSave(coverageDs)
  }

  sparkTest("transform variants to feature rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(features: FeatureRDD) {
      val tempPath = tmpLocation(".bed")
      features.save(tempPath, false, false)

      assert(sc.loadFeatures(tempPath).rdd.count === 6)
    }

    val features: FeatureRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.featFn)
    })

    checkSave(features)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val featureDs: FeatureRDD = variants.transmuteDataset(ds => {
      ds.map(r => {
        FeatureProduct.fromAvro(
          VariantRDDSuite.featFn(r.toAvro))
      })
    })

    checkSave(featureDs)
  }

  sparkTest("transform variants to fragment rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(fragments: FragmentRDD) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 6)
    }

    val fragments: FragmentRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.fragFn)
    })

    checkSave(fragments)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val fragmentsDs: FragmentRDD = variants.transmuteDataset(ds => {
      ds.map(r => {
        FragmentProduct.fromAvro(
          VariantRDDSuite.fragFn(r.toAvro))
      })
    })

    checkSave(fragmentsDs)
  }

  sparkTest("transform variants to read rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(reads: AlignmentRecordRDD) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 6)
    }

    val reads: AlignmentRecordRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.readFn)
    })

    checkSave(reads)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val readsDs: AlignmentRecordRDD = variants.transmuteDataset(ds => {
      ds.map(r => {
        AlignmentRecordProduct.fromAvro(
          VariantRDDSuite.readFn(r.toAvro))
      })
    })

    checkSave(readsDs)
  }

  sparkTest("transform variants to genotype rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(genotypes: GenotypeRDD) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 6)
    }

    val genotypes: GenotypeRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.genFn)
    })

    checkSave(genotypes)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val genotypesDs: GenotypeRDD = variants.transmuteDataset(ds => {
      ds.map(r => {
        GenotypeProduct.fromAvro(
          VariantRDDSuite.genFn(r.toAvro))
      })
    })

    checkSave(genotypesDs)
  }

  sparkTest("transform variants to variant context rdd") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(variantContexts: VariantContextRDD) {
      assert(variantContexts.rdd.count === 6)
    }

    val variantContexts: VariantContextRDD = variants.transmute(rdd => {
      rdd.map(VariantRDDSuite.vcFn)
    })

    checkSave(variantContexts)
  }
}
