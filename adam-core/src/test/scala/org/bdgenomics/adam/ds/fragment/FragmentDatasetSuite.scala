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
package org.bdgenomics.adam.ds.fragment

import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models.{
  Coverage,
  ReadGroup,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.ds.read.{
  AlignmentDataset,
  AlignmentDatasetSuite,
  AnySAMOutFormatter,
  QualityScoreBin
}
import org.bdgenomics.adam.ds.sequence.SliceDataset
import org.bdgenomics.adam.ds.variant.{
  GenotypeDataset,
  VariantDataset,
  VariantContextDataset
}
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
import scala.collection.JavaConversions._

object FragmentDatasetSuite extends Serializable {

  def readFn(f: Fragment): Alignment = {
    f.getAlignments.get(0)
  }

  def vcFn(f: Fragment): VariantContext = {
    VariantContext(AlignmentDatasetSuite.varFn(f))
  }
}

class FragmentDatasetSuite extends ADAMFunSuite {

  sparkTest("don't lose any reads when piping interleaved fastq to sam") {
    // write suffixes at end of reads
    sc.hadoopConfiguration.setBoolean(FragmentDataset.WRITE_SUFFIXES, true)

    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath)
    val records = ardd.rdd.count
    assert(records === 3)
    assert(ardd.dataset.count === 3)
    assert(ardd.dataset.rdd.count === 3)

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts interleaved fastq to unaligned sam
    val scriptPath = testFile("fastq_to_usam.py")

    val pipedRdd: AlignmentDataset = ardd.pipe[Alignment, AlignmentProduct, AlignmentDataset, InterleavedFASTQInFormatter](Seq("python", "$0"),
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(2 * records === newRecords)
  }

  sparkTest("don't lose any reads when piping tab5 to sam") {
    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath)
    val records = ardd.rdd.count
    assert(records === 3)

    implicit val tFormatter = Tab5InFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts tab5 to unaligned sam
    val scriptPath = testFile("tab5_to_usam.py")

    val pipedRdd: AlignmentDataset = ardd.pipe[Alignment, AlignmentProduct, AlignmentDataset, Tab5InFormatter](Seq("python", "$0"),
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(2 * records === newRecords)
  }

  sparkTest("don't lose any reads when piping tab6 to sam") {
    // write suffixes at end of reads
    sc.hadoopConfiguration.setBoolean(FragmentDataset.WRITE_SUFFIXES, true)

    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath)
    val records = ardd.rdd.count
    assert(records === 3)

    implicit val tFormatter = Tab6InFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts tab6 to unaligned sam
    val scriptPath = testFile("tab6_to_usam.py")

    val pipedRdd: AlignmentDataset = ardd.pipe[Alignment, AlignmentProduct, AlignmentDataset, Tab6InFormatter](Seq("python", "$0"),
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(2 * records === newRecords)
  }

  sparkTest("use broadcast join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = fragments.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 5)
  }

  sparkTest("use right outer broadcast join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = fragments.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
  }

  sparkTest("use shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform((rdd: RDD[Fragment]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = fragments.shuffleRegionJoin(targets)
    val jRdd0 = fragments.shuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 5)
    assert(jRdd0.rdd.count === 5)

    val joinedFragments: FragmentDataset = jRdd
      .transmute[Fragment, FragmentProduct, FragmentDataset]((rdd: RDD[(Fragment, Feature)]) => {
        rdd.map(_._1)
      })
    val tempPath = tmpLocation(".adam")
    joinedFragments.saveAsParquet(tempPath)
    assert(sc.loadFragments(tempPath).rdd.count === 5)
  }

  sparkTest("use right outer shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform((rdd: RDD[Fragment]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = fragments.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = fragments.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
    assert(c0.count(_._1.isDefined) === 5)
  }

  sparkTest("use left outer shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform((rdd: RDD[Fragment]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = fragments.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = fragments.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 15)
    assert(c0.count(_._2.isEmpty) === 15)
    assert(c.count(_._2.isDefined) === 5)
    assert(c0.count(_._2.isDefined) === 5)
  }

  sparkTest("use full outer shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform((rdd: RDD[Fragment]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = fragments.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = fragments.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 5)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 5)
  }

  sparkTest("use shuffle join with group by to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform((rdd: RDD[Fragment]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = fragments.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = fragments.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 5)
    assert(c0.size === 5)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform((rdd: RDD[Fragment]) => rdd.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = fragments.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = fragments.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isDefined) === 20)
    assert(c0.count(_._1.isDefined) === 20)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  sparkTest("bin quality scores in fragments") {
    val fragments = sc.loadFragments(testFile("bqsr1.sam"))
    val binnedFragments = fragments.binQualityScores(Seq(QualityScoreBin(0, 20, 10),
      QualityScoreBin(20, 40, 30),
      QualityScoreBin(40, 60, 50)))
    val qualityScoreCounts = binnedFragments.rdd.flatMap(fragment => {
      fragment.getAlignments.toSeq
    }).flatMap(read => {
      read.getQualityScores
    }).map(s => s.toInt - 33)
      .countByValue

    assert(qualityScoreCounts(30) === 92899)
    assert(qualityScoreCounts(10) === 7101)
  }

  sparkTest("union two genomic datasets of fragments together") {
    val reads1 = sc.loadAlignments(testFile("bqsr1.sam")).toFragments
    val reads2 = sc.loadAlignments(testFile("small.sam")).toFragments
    val union = reads1.union(reads2)
    assert(union.rdd.count === (reads1.rdd.count + reads2.rdd.count))
    // all of the contigs small.sam has are in bqsr1.sam
    assert(union.references.size === reads1.references.size)
    // small.sam has no read groups
    assert(union.readGroups.size === reads1.readGroups.size)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    def testMetadata(fRdd: FragmentDataset) {
      val sequenceRdd = fRdd.addReference(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.references.containsReferenceName("aSequence"))

      val rgDataset = fRdd.addReadGroup(ReadGroup("test", "aRg"))
      assert(rgDataset.readGroups("aRg").sampleId === "test")
    }

    val inputPath = testFile("small.sam")
    val outputPath = tmpLocation()
    val rrdd = sc.loadFragments(inputPath)
    testMetadata(rrdd)
    val rdd = rrdd.transformDataset(ds => ds) // no-op, force conversion to ds
    testMetadata(rdd)
    assert(rdd.dataset.count === 20)
    assert(rdd.rdd.count === 20)
    rdd.saveAsParquet(outputPath)
    val rdd2 = sc.loadFragments(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 20)
    assert(rdd2.dataset.count === 20)
    val outputPath2 = tmpLocation()
    rdd.transform((rdd: RDD[Fragment]) => rdd) // no-op but force to rdd
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadFragments(outputPath2)
    assert(rdd3.rdd.count === 20)
    assert(rdd3.dataset.count === 20)
    val outputPath3 = tmpLocation()
    rdd3.save(outputPath3)
    val rdd4 = sc.loadFragments(outputPath3)
    assert(rdd4.rdd.count === 20)
    assert(rdd4.dataset.count === 20)
  }

  sparkTest("transform fragments to slice genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(sliceRdd: SliceDataset) {
      val tempPath = tmpLocation(".fa")
      sliceRdd.saveAsFasta(tempPath)

      assert(sc.loadSlices(tempPath).rdd.count.toInt === 20)
    }

    val slices: SliceDataset = fragments.transmute[Slice, SliceProduct, SliceDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(AlignmentDatasetSuite.sliceFn)
      })

    checkSave(slices)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val slicesDs: SliceDataset = fragments.transmuteDataset[Slice, SliceProduct, SliceDataset](
      (ds: Dataset[FragmentProduct]) => {
        ds.map(r => {
          SliceProduct.fromAvro(
            AlignmentDatasetSuite.sliceFn(r.toAvro))
        })
      })

    checkSave(slicesDs)
  }

  sparkTest("transform fragments to coverage genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(coverage: CoverageDataset) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 20)
    }

    val coverage: CoverageDataset = fragments.transmute[Coverage, Coverage, CoverageDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(AlignmentDatasetSuite.covFn)
      })

    checkSave(coverage)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val coverageDs: CoverageDataset = fragments.transmuteDataset[Coverage, Coverage, CoverageDataset](
      (ds: Dataset[FragmentProduct]) => {
        ds.map(r => AlignmentDatasetSuite.covFn(r.toAvro))
      })

    checkSave(coverageDs)
  }

  sparkTest("transform fragments to feature genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(features: FeatureDataset) {
      val tempPath = tmpLocation(".bed")
      features.saveAsBed(tempPath)

      assert(sc.loadFeatures(tempPath).rdd.count === 20)
    }

    val features: FeatureDataset = fragments.transmute[Feature, FeatureProduct, FeatureDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(AlignmentDatasetSuite.featFn)
      })

    checkSave(features)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val featuresDs: FeatureDataset = fragments.transmuteDataset[Feature, FeatureProduct, FeatureDataset](
      (ds: Dataset[FragmentProduct]) => {
        ds.map(r => {
          FeatureProduct.fromAvro(
            AlignmentDatasetSuite.featFn(r.toAvro))
        })
      })

    checkSave(featuresDs)
  }

  sparkTest("transform fragments to read genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(reads: AlignmentDataset) {
      val tempPath = tmpLocation(".sam")
      reads.saveAsSam(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 20)
    }

    val reads: AlignmentDataset = fragments.transmute[Alignment, AlignmentProduct, AlignmentDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(FragmentDatasetSuite.readFn)
      })

    checkSave(reads)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val readDs: AlignmentDataset = fragments.transmuteDataset[Alignment, AlignmentProduct, AlignmentDataset](
      (ds: Dataset[FragmentProduct]) => {
        ds.map(r => {
          AlignmentProduct.fromAvro(
            FragmentDatasetSuite.readFn(r.toAvro))
        })
      })

    checkSave(readDs)
  }

  sparkTest("transform fragments to genotype genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 20)
    }

    val genotypes: GenotypeDataset = fragments.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(AlignmentDatasetSuite.genFn)
      })

    checkSave(genotypes)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val genotypesDs: GenotypeDataset = fragments.transmuteDataset[Genotype, GenotypeProduct, GenotypeDataset](
      (ds: Dataset[FragmentProduct]) => {
        ds.map(r => {
          GenotypeProduct.fromAvro(
            AlignmentDatasetSuite.genFn(r.toAvro))
        })
      })

    checkSave(genotypesDs)
  }

  sparkTest("transform fragments to variant genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(variants: VariantDataset) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 20)
    }

    val variants: VariantDataset = fragments.transmute[Variant, VariantProduct, VariantDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(AlignmentDatasetSuite.varFn)
      })

    checkSave(variants)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val variantsDs: VariantDataset = fragments.transmuteDataset[Variant, VariantProduct, VariantDataset](
      (ds: Dataset[FragmentProduct]) => {
        ds.map(r => {
          VariantProduct.fromAvro(
            AlignmentDatasetSuite.varFn(r.toAvro))
        })
      })

    checkSave(variantsDs)
  }

  sparkTest("transform fragments to variant context genomic dataset") {
    val fragments = sc.loadFragments(testFile("small.sam"))

    def checkSave(variantContexts: VariantContextDataset) {
      assert(variantContexts.rdd.count === 20)
    }

    val variantContexts: VariantContextDataset = fragments.transmute[VariantContext, VariantContextProduct, VariantContextDataset](
      (rdd: RDD[Fragment]) => {
        rdd.map(FragmentDatasetSuite.vcFn)
      })

    checkSave(variantContexts)
  }

  sparkTest("paired read names with index sequences in read names can group into fragments") {
    val path1 = testFile("read_names_with_index_sequences_pair1.fq")
    val path2 = testFile("read_names_with_index_sequences_pair2.fq")
    val fragments = sc.loadPairedFastq(path1, path2).toFragments()

    assert(fragments.rdd.count() == 4)

    fragments.rdd.collect().foreach(fragment => {
      assert(fragment.getAlignments.size() == 2)
    })
  }

  sparkTest("interleaved paired read names with index sequences in read names can group into fragments") {
    val path = testFile("read_names_with_index_sequences_interleaved.fq")
    val fragments = sc.loadInterleavedFastq(path).toFragments()

    assert(fragments.rdd.count() == 4)

    fragments.rdd.collect().foreach(fragment => {
      assert(fragment.getAlignments.size() == 2)
    })
  }

  sparkTest("interleaved paired read names with index sequences in read names as fragments") {
    val path = testFile("read_names_with_index_sequences_interleaved.fq")
    val fragments = sc.loadInterleavedFastqAsFragments(path)

    assert(fragments.rdd.count() == 4)

    fragments.rdd.collect().foreach(fragment => {
      assert(fragment.getAlignments.size() == 2)
    })
  }

  sparkTest("transform dataset via java API") {
    val path = testFile("read_names_with_index_sequences_interleaved.fq")
    val fragments = sc.loadInterleavedFastqAsFragments(path)

    val transformed = fragments.transformDataset(new JFunction[Dataset[FragmentProduct], Dataset[FragmentProduct]]() {
      override def call(ds: Dataset[FragmentProduct]): Dataset[FragmentProduct] = {
        ds
      }
    })

    assert(fragments.dataset.first().alignments.size === transformed.dataset.first().alignments.size)
  }

  sparkTest("dataset and rdd conversion to alignments are equivalent") {
    val fragments = sc.loadFragments(testFile("small.sam"))
    val fragmentRdd = RDDBoundFragmentDataset(fragments.rdd, fragments.references,
      fragments.readGroups, fragments.processingSteps, None)
    val fragmentDataset = DatasetBoundFragmentDataset(fragments.dataset, fragments.references,
      fragments.readGroups, fragments.processingSteps)
    val convertedRdd = fragmentRdd.toAlignments()
    val convertedDataset = fragmentDataset.toAlignments()

    assert(convertedRdd.rdd.collect().toSet == convertedDataset.rdd.collect().toSet)
  }
}
