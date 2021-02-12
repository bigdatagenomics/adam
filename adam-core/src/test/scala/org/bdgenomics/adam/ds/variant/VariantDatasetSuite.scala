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
package org.bdgenomics.adam.ds.variant

import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models.{
  Coverage,
  ReferenceRegion,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.ds.sequence.SliceDataset
import org.bdgenomics.adam.sql.{
  Alignment => AlignmentProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  Slice => SliceProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

object VariantDatasetSuite extends Serializable {

  def covFn(v: Variant): Coverage = {
    Coverage(v.getReferenceName,
      v.getStart,
      v.getEnd,
      1)
  }

  def covFn(vc: VariantContext): Coverage = {
    covFn(vc.variant.variant)
  }

  def featFn(v: Variant): Feature = {
    Feature.newBuilder
      .setReferenceName(v.getReferenceName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build
  }

  def featFn(vc: VariantContext): Feature = {
    featFn(vc.variant.variant)
  }

  def fragFn(v: Variant): Fragment = {
    Fragment.newBuilder
      .setName(v.getReferenceName)
      .build
  }

  def fragFn(vc: VariantContext): Fragment = {
    fragFn(vc.variant.variant)
  }

  def sliceFn(v: Variant): Slice = {
    Slice.newBuilder
      .setName(v.getReferenceName)
      .build
  }

  def sliceFn(vc: VariantContext): Slice = {
    sliceFn(vc.variant.variant)
  }

  def readFn(v: Variant): Alignment = {
    Alignment.newBuilder
      .setReferenceName(v.getReferenceName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build
  }

  def readFn(vc: VariantContext): Alignment = {
    readFn(vc.variant.variant)
  }

  def genFn(v: Variant): Genotype = {
    Genotype.newBuilder
      .setReferenceName(v.getReferenceName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build
  }

  def genFn(vc: VariantContext): Genotype = {
    genFn(vc.variant.variant)
  }

  def vcFn(v: Variant): VariantContext = {
    VariantContext(Variant.newBuilder
      .setReferenceName(v.getReferenceName)
      .setStart(v.getStart)
      .setEnd(v.getEnd)
      .build)
  }
}

class VariantDatasetSuite extends ADAMFunSuite {

  sparkTest("union two variant genomic datasets together") {
    val variant1 = sc.loadVariants(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
    val variant2 = sc.loadVariants(testFile("small.vcf"))
    val union = variant1.union(variant2)
    assert(union.rdd.count === (variant1.rdd.count + variant2.rdd.count))
    assert(union.references.size === (variant1.references.size + variant2.references.size))
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

  sparkTest("save and reload from partitioned parquet") {
    val variants = sc.loadVariants(testFile("sorted-variants.vcf"))
    val outputPath = tmpLocation()
    variants.saveAsPartitionedParquet(outputPath, partitionSize = 1000000)
    val unfilteredVariants = sc.loadPartitionedParquetVariants(outputPath)
    assert(unfilteredVariants.rdd.count === 6)
    assert(unfilteredVariants.dataset.count === 6)

    val regionsVariants = sc.loadPartitionedParquetVariants(outputPath,
      List(ReferenceRegion("2", 19000L, 21000L),
        ReferenceRegion("13", 752700L, 752750L)))
    assert(regionsVariants.rdd.count === 2)
    assert(regionsVariants.dataset.count === 2)
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
      .transform((rdd: RDD[Variant]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = variants.shuffleRegionJoin(targets)
    val jRdd0 = variants.shuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 3L)
    assert(jRdd0.rdd.count === 3L)

    val joinedVariants: VariantDataset = jRdd
      .transmute[Variant, VariantProduct, VariantDataset]((rdd: RDD[(Variant, Feature)]) => {
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
      .transform((rdd: RDD[Variant]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

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
      .transform((rdd: RDD[Variant]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

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
      .transform((rdd: RDD[Variant]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

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
      .transform((rdd: RDD[Variant]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

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
      .transform((rdd: RDD[Variant]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

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

    assert(variantContexts.references.containsReferenceName("1"))
    assert(variantContexts.samples.isEmpty)

    val vcs = variantContexts.rdd.collect
    assert(vcs.size === 6)

    val vc = vcs.head
    assert(vc.position.referenceName === "1")
    assert(vc.variant.variant.getReferenceName === "1")
    assert(vc.genotypes.isEmpty)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    def testMetadata(vRdd: VariantDataset) {
      val sequenceRdd = vRdd.addReference(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.references.containsReferenceName("aSequence"))

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
    val rdd2 = sc.loadVariants(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 6)
    assert(rdd2.dataset.count === 6)
    val outputPath2 = tmpLocation()
    rdd.transform((rdd: RDD[Variant]) => rdd) // no-op but force to rdd
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadVariants(outputPath2)
    assert(rdd3.rdd.count === 6)
    assert(rdd3.dataset.count === 6)
  }

  sparkTest("transform variants to slice genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(slices: SliceDataset) {
      val tempPath = tmpLocation(".adam")
      slices.saveAsParquet(tempPath)

      assert(sc.loadSlices(tempPath).rdd.count === 6)
    }

    val slices: SliceDataset = variants.transmute[Slice, SliceProduct, SliceDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.sliceFn)
      })

    checkSave(slices)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val slicesDs: SliceDataset = variants.transmuteDataset[Slice, SliceProduct, SliceDataset](
      (ds: Dataset[VariantProduct]) => {
        ds.map(r => {
          SliceProduct.fromAvro(
            VariantDatasetSuite.sliceFn(r.toAvro))
        })
      })

    checkSave(slicesDs)
  }

  sparkTest("transform variants to coverage genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(coverage: CoverageDataset) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 6)
    }

    val coverage: CoverageDataset = variants.transmute[Coverage, Coverage, CoverageDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.covFn)
      })

    checkSave(coverage)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val coverageDs: CoverageDataset = variants.transmuteDataset[Coverage, Coverage, CoverageDataset](
      (ds: Dataset[VariantProduct]) => {
        ds.map(r => VariantDatasetSuite.covFn(r.toAvro))
      })

    checkSave(coverageDs)
  }

  sparkTest("transform variants to feature genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(features: FeatureDataset) {
      val tempPath = tmpLocation(".bed")
      features.save(tempPath, false, false)

      assert(sc.loadFeatures(tempPath).rdd.count === 6)
    }

    val features: FeatureDataset = variants.transmute[Feature, FeatureProduct, FeatureDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.featFn)
      })

    checkSave(features)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val featureDs: FeatureDataset = variants.transmuteDataset[Feature, FeatureProduct, FeatureDataset](
      (ds: Dataset[VariantProduct]) => {
        ds.map(r => {
          FeatureProduct.fromAvro(
            VariantDatasetSuite.featFn(r.toAvro))
        })
      })

    checkSave(featureDs)
  }

  sparkTest("transform variants to fragment genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(fragments: FragmentDataset) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 6)
    }

    val fragments: FragmentDataset = variants.transmute[Fragment, FragmentProduct, FragmentDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.fragFn)
      })

    checkSave(fragments)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val fragmentsDs: FragmentDataset = variants.transmuteDataset[Fragment, FragmentProduct, FragmentDataset](
      (ds: Dataset[VariantProduct]) => {
        ds.map(r => {
          FragmentProduct.fromAvro(
            VariantDatasetSuite.fragFn(r.toAvro))
        })
      })

    checkSave(fragmentsDs)
  }

  sparkTest("transform variants to read genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(reads: AlignmentDataset) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 6)
    }

    val reads: AlignmentDataset = variants.transmute[Alignment, AlignmentProduct, AlignmentDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.readFn)
      })

    checkSave(reads)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val readsDs: AlignmentDataset = variants.transmuteDataset[Alignment, AlignmentProduct, AlignmentDataset](
      (ds: Dataset[VariantProduct]) => {
        ds.map(r => {
          AlignmentProduct.fromAvro(
            VariantDatasetSuite.readFn(r.toAvro))
        })
      })

    checkSave(readsDs)
  }

  sparkTest("transform variants to genotype genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 6)
    }

    val genotypes: GenotypeDataset = variants.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.genFn)
      })

    checkSave(genotypes)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val genotypesDs: GenotypeDataset = variants.transmuteDataset[Genotype, GenotypeProduct, GenotypeDataset](
      (ds: Dataset[VariantProduct]) => {
        ds.map(r => {
          GenotypeProduct.fromAvro(
            VariantDatasetSuite.genFn(r.toAvro))
        })
      })

    checkSave(genotypesDs)
  }

  sparkTest("transform variants to variant context genomic dataset") {
    val variants = sc.loadVariants(testFile("small.vcf"))

    def checkSave(variantContexts: VariantContextDataset) {
      assert(variantContexts.rdd.count === 6)
    }

    val variantContexts: VariantContextDataset = variants.transmute[VariantContext, VariantContextProduct, VariantContextDataset](
      (rdd: RDD[Variant]) => {
        rdd.map(VariantDatasetSuite.vcFn)
      })

    checkSave(variantContexts)
  }

  sparkTest("filter RDD bound variants to filters passed") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    assert(variants.filterToFiltersPassed().rdd.count() === 4)
  }

  sparkTest("filter dataset bound variants to filters passed") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterToFiltersPassed().dataset.count() === 4)
  }

  sparkTest("filter RDD bound variants by quality") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    assert(variants.filterByQuality(2000.0d).rdd.count() === 3)
  }

  sparkTest("filter dataset bound variants by quality") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterByQuality(2000.0d).dataset.count() === 3)
  }

  sparkTest("filter RDD bound variants by read depth") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    assert(variants.filterByReadDepth(20).rdd.count() === 0)
  }

  sparkTest("filter dataset bound variants by read depth") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterByReadDepth(20).dataset.count() === 0)
  }

  sparkTest("filter RDD bound variants by reference read depth") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    assert(variants.filterByReferenceReadDepth(20).rdd.count() === 0)
  }

  sparkTest("filter dataset bound variants by reference read depth") {
    val variants = sc.loadVariants(testFile("random.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterByReferenceReadDepth(20).dataset.count() === 0)
  }

  sparkTest("filter RDD bound single nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    assert(variants.filterSingleNucleotideVariants().rdd.count() === 3)
  }

  sparkTest("filter dataset bound single nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterSingleNucleotideVariants().dataset.count() === 3)
  }

  sparkTest("filter RDD bound multiple nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    assert(variants.filterMultipleNucleotideVariants().rdd.count() === 18)
  }

  sparkTest("filter dataset bound multiple nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterMultipleNucleotideVariants().dataset.count() === 18)
  }

  sparkTest("filter RDD bound indel variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    assert(variants.filterIndels().rdd.count() === 17)
  }

  sparkTest("filter dataset bound indel variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterIndels().dataset.count() === 17)
  }

  sparkTest("filter RDD bound variants to single nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    assert(variants.filterToSingleNucleotideVariants().rdd.count() === 16)
  }

  sparkTest("filter dataset bound variants to single nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterToSingleNucleotideVariants().dataset.count() === 16)
  }

  sparkTest("filter RDD bound variants to multiple nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    assert(variants.filterToMultipleNucleotideVariants().rdd.count() === 1)
  }

  sparkTest("filter dataset bound variants to multiple nucleotide variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterToMultipleNucleotideVariants().dataset.count() === 1)
  }

  sparkTest("filter RDD bound variants to indel variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    assert(variants.filterToIndels().rdd.count() === 2)
  }

  sparkTest("filter dataset bound variants to indel variants") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf"))
    val variantsDs = variants.transformDataset(ds => ds)
    assert(variantsDs.filterToIndels().dataset.count() === 2)
  }

  sparkTest("transform dataset via java API") {
    val variants = sc.loadVariants(testFile("NA12878.chr22.tiny.freebayes.vcf")).sort()

    val transformed = variants.transformDataset(new JFunction[Dataset[VariantProduct], Dataset[VariantProduct]]() {
      override def call(ds: Dataset[VariantProduct]): Dataset[VariantProduct] = {
        ds
      }
    })

    assert(variants.dataset.first().start.get === transformed.dataset.first().start.get)
  }
}
