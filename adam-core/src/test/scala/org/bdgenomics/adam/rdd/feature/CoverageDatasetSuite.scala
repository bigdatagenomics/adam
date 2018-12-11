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
package org.bdgenomics.adam.rdd.feature

import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  Coverage,
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentDataset
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset
import org.bdgenomics.adam.rdd.variant.{
  GenotypeDataset,
  VariantDataset,
  VariantContextDataset
}
import org.bdgenomics.adam.sql.{
  AlignmentRecord => AlignmentRecordProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  NucleotideContigFragment => NucleotideContigFragmentProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

object CoverageDatasetSuite extends Serializable {

  def ncfFn(cov: Coverage): NucleotideContigFragment = {
    NucleotideContigFragment.newBuilder
      .setContigName(cov.referenceName)
      .setStart(cov.start)
      .setEnd(cov.end)
      .build
  }

  def featFn(cov: Coverage): Feature = {
    Feature.newBuilder
      .setReferenceName(cov.referenceName)
      .setStart(cov.start)
      .setEnd(cov.end)
      .build
  }

  def fragFn(cov: Coverage): Fragment = {
    Fragment.newBuilder
      .setName(cov.referenceName)
      .build
  }

  def genFn(cov: Coverage): Genotype = {
    Genotype.newBuilder
      .setReferenceName(cov.referenceName)
      .setStart(cov.start)
      .setEnd(cov.end)
      .build
  }

  def readFn(cov: Coverage): AlignmentRecord = {
    AlignmentRecord.newBuilder
      .setReferenceName(cov.referenceName)
      .setStart(cov.start)
      .setEnd(cov.end)
      .build
  }

  def varFn(cov: Coverage): Variant = {
    Variant.newBuilder
      .setReferenceName(cov.referenceName)
      .setStart(cov.start)
      .setEnd(cov.end)
      .build
  }

  def vcFn(cov: Coverage): VariantContext = {
    VariantContext(Variant.newBuilder
      .setReferenceName(cov.referenceName)
      .setStart(cov.start)
      .setEnd(cov.end)
      .build)
  }
}

class CoverageDatasetSuite extends ADAMFunSuite {

  val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L)))

  def generateCoverage(length: Int): Seq[Coverage] = {
    // generate adjacent regions with coverage
    var j = 0
    val coverage =
      (0 until length).map(i => {
        if ((i % 4) == 0) {
          j = j + 1
        }
        Coverage("chr1", i, i + 1, j.toDouble)
      })
    coverage.toSeq
  }

  sparkTest("correctly saves coverage") {
    def testMetadata(cRdd: CoverageDataset) {
      val sequenceRdd = cRdd.addSequence(SequenceRecord("aSequence", 1000L))
      val sampleRdd = cRdd.addSample(Sample.newBuilder().setName("Sample").build())
      assert(sequenceRdd.sequences.containsReferenceName("aSequence"))
      assert(sampleRdd.samples.map(r => r.getName).contains("Sample"))
    }

    val f1 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setReferenceName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setReferenceName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featuresDs: FeatureDataset = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val coverageDs: CoverageDataset = featuresDs.toCoverage
    testMetadata(coverageDs)

    val outputFile = tmpLocation(".bed")
    coverageDs.save(outputFile, false, false)

    val coverage = sc.loadCoverage(outputFile)
    testMetadata(coverage)
    assert(coverage.rdd.count == 3)
    assert(coverage.dataset.count == 3)

    // go to dataset and save as parquet
    val outputFile2 = tmpLocation(".adam")
    val dsCov = coverage.transformDataset(ds => ds)
    testMetadata(dsCov)
    dsCov.save(outputFile2, false, false)
    val coverage2 = sc.loadCoverage(outputFile2)
    testMetadata(coverage2)
    assert(coverage2.rdd.count == 3)
    assert(coverage2.dataset.count == 3)

    // load as features, force to dataset, convert to coverage, and count
    val features2Ds = sc.loadFeatures(outputFile2)
      .transformDataset(ds => ds) // no-op, force to dataset
    val coverage2Ds = features2Ds.toCoverage
    assert(coverage2Ds.rdd.count == 3)
    assert(coverage2Ds.dataset.count == 3)

    // translate to features and count
    val features2 = coverage2.toFeatures
    assert(features2.rdd.count == 3)
    assert(features2.dataset.count == 3)

    // go to rdd and save as parquet
    val outputFile3 = tmpLocation(".adam")
    coverageDs.transform(rdd => rdd).save(outputFile3, false, false)
    val coverage3 = sc.loadCoverage(outputFile3)
    assert(coverage3.rdd.count == 3)
    assert(coverage3.dataset.count == 3)
  }

  sparkTest("can read a bed file to coverage") {
    val inputPath = testFile("sample_coverage.bed")
    val coverage = sc.loadCoverage(inputPath)
    assert(coverage.rdd.count() == 3)
    assert(coverage.dataset.count() == 3)
    val selfUnion = coverage.union(coverage)
    assert(selfUnion.rdd.count === 6)
    val coverageDs = coverage.transformDataset(ds => ds) // no-op, forces to dataset
    val selfUnionDs = coverageDs.union(coverageDs)
    assert(selfUnionDs.rdd.count === 6)
  }

  sparkTest("correctly filters coverage with predicate") {
    val f1 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setReferenceName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setReferenceName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featureDs: FeatureDataset = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val coverageDs: CoverageDataset = featureDs.toCoverage

    val outputFile = tmpLocation(".adam")
    coverageDs.save(outputFile, false, false)

    val region = ReferenceRegion("chr1", 1, 9)
    val predicate = region.toPredicate
    val coverage = sc.loadParquetCoverage(outputFile, Some(predicate))
    assert(coverage.rdd.count == 1)
  }

  sparkTest("keeps sample metadata") {
    val cov = generateCoverage(20)

    val sample1 = Sample.newBuilder()
      .setName("Sample1")
      .build()

    val sample2 = Sample.newBuilder()
      .setName("Sample2")
      .build()

    val c1 = RDDBoundCoverageDataset(sc.parallelize(cov.toSeq).repartition(1), sd, Seq(sample1), None)
    val c2 = RDDBoundCoverageDataset(sc.parallelize(cov.toSeq).repartition(1), sd, Seq(sample2), None)

    val union = c1.union(c2)
    assert(union.samples.size === 2)
  }

  sparkTest("can read a bed file with multiple samples to coverage") {

    val f1 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(10).setScore(3.0).setSampleId("S1").build()
    val f2 = Feature.newBuilder().setReferenceName("chr1").setStart(15).setEnd(20).setScore(2.0).setSampleId("S1").build()
    val f3 = Feature.newBuilder().setReferenceName("chr2").setStart(15).setEnd(20).setScore(2.0).setSampleId("S1").build()

    val f4 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(10).setScore(2.0).setSampleId("S2").build()
    val f5 = Feature.newBuilder().setReferenceName("chr1").setStart(15).setEnd(20).setScore(2.0).setSampleId("S2").build()

    val featureDs: FeatureDataset = FeatureDataset(sc.parallelize(Seq(f1, f2, f3, f4, f5)))
    val coverageDs: CoverageDataset = featureDs.toCoverage

    val outputFile = tmpLocation(".adam")
    coverageDs.save(outputFile, false, false)

    val region = ReferenceRegion("chr1", 1, 9)
    val predicate = region.toPredicate
    val coverage = sc.loadParquetCoverage(outputFile, Some(predicate))
    assert(coverage.rdd.count == 2)
  }

  sparkTest("correctly flatmaps coverage without aggregated bins") {
    val f1 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(5).setScore(1.0).build()
    val f2 = Feature.newBuilder().setReferenceName("chr1").setStart(5).setEnd(7).setScore(3.0).build()
    val f3 = Feature.newBuilder().setReferenceName("chr1").setStart(7).setEnd(20).setScore(4.0).build()

    val featureDs: FeatureDataset = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val coverageDs: CoverageDataset = featureDs.toCoverage
    val coverage = coverageDs.coverage(bpPerBin = 4)

    assert(coverage.rdd.count == 4)
  }

  sparkTest("correctly flatmaps coverage with aggregated bins") {
    val f1 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(5).setScore(1.0).build()
    val f2 = Feature.newBuilder().setReferenceName("chr1").setStart(5).setEnd(7).setScore(3.0).build()
    val f3 = Feature.newBuilder().setReferenceName("chr1").setStart(7).setEnd(20).setScore(4.0).build()

    val featureDs: FeatureDataset = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val coverageDs: CoverageDataset = featureDs.toCoverage

    val coverage = coverageDs
      .aggregatedCoverage(bpPerBin = 4)

    assert(coverage.rdd.count == 5)
    assert(coverage.rdd.filter(_.start == 4).first.count == 2.75)
    assert(coverage.rdd.filter(_.start == 8).first.count == 4.0)
  }

  sparkTest("collapses coverage records in one partition") {
    val cov = generateCoverage(20)
    val coverage = RDDBoundCoverageDataset(sc.parallelize(cov.toSeq).repartition(1), sd, Seq.empty, None)
    val collapsed = coverage.collapse

    assert(coverage.rdd.count == 20)
    assert(collapsed.rdd.count == 5)
  }

  sparkTest("approximately collapses coverage records in multiple partitions") {
    val cov = generateCoverage(20)
    val coverage = RDDBoundCoverageDataset(sc.parallelize(cov), sd, Seq.empty, None)
    val collapsed = coverage.collapse

    assert(collapsed.rdd.count == 8)
  }

  sparkTest("transform coverage to contig rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(contigs: NucleotideContigFragmentDataset) {
      val tempPath = tmpLocation(".adam")
      contigs.saveAsParquet(tempPath)

      assert(sc.loadContigFragments(tempPath).rdd.count === 3)
    }

    val contigs: NucleotideContigFragmentDataset = coverage.transmute[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.ncfFn)
      })

    checkSave(contigs)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val contigsDs: NucleotideContigFragmentDataset = coverage.transmuteDataset[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset](
      (ds: Dataset[Coverage]) => {
        ds.map(r => {
          NucleotideContigFragmentProduct.fromAvro(
            CoverageDatasetSuite.ncfFn(r))
        })
      })

    checkSave(contigsDs)
  }

  sparkTest("transform coverage to feature rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(features: FeatureDataset) {
      val tempPath = tmpLocation(".bed")
      features.saveAsBed(tempPath)

      assert(sc.loadFeatures(tempPath).rdd.count === 3)
    }

    val features: FeatureDataset = coverage.transmute[Feature, FeatureProduct, FeatureDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.featFn)
      })

    checkSave(features)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val featuresDs: FeatureDataset = coverage.transmuteDataset[Feature, FeatureProduct, FeatureDataset](
      (ds: Dataset[Coverage]) => {
        ds.map(r => {
          FeatureProduct.fromAvro(
            CoverageDatasetSuite.featFn(r))
        })
      })

    checkSave(featuresDs)
  }

  sparkTest("transform coverage to fragment rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(fragments: FragmentDataset) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 3)
    }

    val fragments: FragmentDataset = coverage.transmute[Fragment, FragmentProduct, FragmentDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.fragFn)
      })

    checkSave(fragments)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val fragmentsDs: FragmentDataset = coverage.transmuteDataset[Fragment, FragmentProduct, FragmentDataset](
      (ds: Dataset[Coverage]) => {
        ds.map(r => {
          FragmentProduct.fromAvro(
            CoverageDatasetSuite.fragFn(r))
        })
      })

    checkSave(fragmentsDs)
  }

  sparkTest("transform coverage to read rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(reads: AlignmentRecordDataset) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 3)
    }

    val reads: AlignmentRecordDataset = coverage.transmute[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.readFn)
      })

    checkSave(reads)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val readsDs: AlignmentRecordDataset = coverage.transmuteDataset[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset](
      (ds: Dataset[Coverage]) => {
        ds.map(r => {
          AlignmentRecordProduct.fromAvro(
            CoverageDatasetSuite.readFn(r))
        })
      })

    checkSave(readsDs)
  }

  sparkTest("transform coverage to genotype rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 3)
    }

    val genotypes: GenotypeDataset = coverage.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.genFn)
      })

    checkSave(genotypes)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val genotypesDs: GenotypeDataset = coverage.transmuteDataset[Genotype, GenotypeProduct, GenotypeDataset](
      (ds: Dataset[Coverage]) => {
        ds.map(r => {
          GenotypeProduct.fromAvro(
            CoverageDatasetSuite.genFn(r))
        })
      })

    checkSave(genotypesDs)
  }

  sparkTest("transform coverage to variant rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(variants: VariantDataset) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 3)
    }

    val variants: VariantDataset = coverage.transmute[Variant, VariantProduct, VariantDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.varFn)
      })

    checkSave(variants)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val variantsDs: VariantDataset = coverage.transmuteDataset[Variant, VariantProduct, VariantDataset](
      (ds: Dataset[Coverage]) => {
        ds.map(r => {
          VariantProduct.fromAvro(
            CoverageDatasetSuite.varFn(r))
        })
      })

    checkSave(variantsDs)
  }

  sparkTest("transform coverage to variant context rdd") {
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"))

    def checkSave(variantContexts: VariantContextDataset) {
      assert(variantContexts.rdd.count === 3)
    }

    val variantContexts: VariantContextDataset = coverage.transmute[VariantContext, VariantContextProduct, VariantContextDataset](
      (rdd: RDD[Coverage]) => {
        rdd.map(CoverageDatasetSuite.vcFn)
      })

    checkSave(variantContexts)
  }

  sparkTest("copy coverage rdd") {
    val sd = sc.loadSequenceDictionary(testFile("hg19.genome"))
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"), optSequenceDictionary = Some(sd))
    assert(coverage.sequences.containsReferenceName("chr1"))

    val copy = CoverageDataset.apply(coverage.rdd, coverage.sequences, Seq.empty)
    assert(copy.rdd.count() === coverage.rdd.count())
    assert(copy.sequences.containsReferenceName("chr1"))
  }

  sparkTest("copy coverage dataset") {
    val sd = sc.loadSequenceDictionary(testFile("hg19.genome"))
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"), optSequenceDictionary = Some(sd))
    assert(coverage.sequences.containsReferenceName("chr1"))

    val copy = CoverageDataset.apply(coverage.dataset, coverage.sequences, coverage.samples)
    assert(copy.dataset.count() === coverage.dataset.count())
    assert(copy.sequences.containsReferenceName("chr1"))
  }

  sparkTest("copy coverage rdd without sequence dictionary") {
    val sd = sc.loadSequenceDictionary(testFile("hg19.genome"))
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"), optSequenceDictionary = Some(sd))
    assert(coverage.sequences.containsReferenceName("chr1"))

    val copy = CoverageDataset.apply(coverage.rdd)
    assert(copy.rdd.count() === coverage.rdd.count())
    assert(copy.sequences.containsReferenceName("chr1") === false)
  }

  sparkTest("copy coverage dataset without sequence dictionary") {
    val sd = sc.loadSequenceDictionary(testFile("hg19.genome"))
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"), optSequenceDictionary = Some(sd))
    assert(coverage.sequences.containsReferenceName("chr1"))

    val copy = CoverageDataset.apply(coverage.dataset)
    assert(copy.dataset.count() === coverage.dataset.count())
    assert(copy.sequences.containsReferenceName("chr1") == false)
  }

  sparkTest("transform dataset via java API") {
    val sd = sc.loadSequenceDictionary(testFile("hg19.genome"))
    val coverage = sc.loadCoverage(testFile("sample_coverage.bed"), optSequenceDictionary = Some(sd))

    val transformed = coverage.transformDataset(new JFunction[Dataset[Coverage], Dataset[Coverage]]() {
      override def call(ds: Dataset[Coverage]): Dataset[Coverage] = {
        ds
      }
    })

    assert(coverage.dataset.first().start === transformed.dataset.first().start)
  }
}

