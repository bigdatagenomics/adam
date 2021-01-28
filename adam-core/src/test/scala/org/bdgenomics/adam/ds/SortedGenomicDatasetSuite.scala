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
package org.bdgenomics.adam.ds

import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{ SequenceRecord, SequenceDictionary, ReferenceRegion }
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.ds.feature.FeatureDataset
import org.bdgenomics.adam.ds.variant.GenotypeDataset
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.SparkFunSuite
import scala.collection.mutable.ListBuffer

class SortedGenomicDatasetSuite extends SparkFunSuite {

  /**
   * Determines if a given partition map has been correctly sorted
   *
   * @param list The partition map
   * @return a boolean where true is sorted and false is unsorted
   */
  def isSorted(list: Seq[Option[(ReferenceRegion, ReferenceRegion)]]): Boolean = {
    val test = list.drop(1).map(_.get._1)
    val test2 = list.dropRight(1).map(_.get._2)
    !test2.zip(test).exists(f => f._1.start > f._2.start && f._1.end > f._2.end && f._1.referenceName > f._2.referenceName)
  }
  val chromosomeLengths = Map(1 -> 248956422, 2 -> 242193529, 3 -> 198295559, 4 -> 190214555, 5 -> 181538259, 6 -> 170805979, 7 -> 159345973, 8 -> 145138636, 9 -> 138394717, 10 -> 133797422, 11 -> 135086622, 12 -> 133275309, 13 -> 114364328, 14 -> 107043718, 15 -> 101991189, 16 -> 90338345, 17 -> 83257441, 18 -> 80373285, 19 -> 58617616, 20 -> 64444167, 21 -> 46709983, 22 -> 50818468)

  val sd = new SequenceDictionary(Vector(
    SequenceRecord("chr20", 63025520),
    SequenceRecord("chr7", 159138663),
    SequenceRecord("chr18", 78077248),
    SequenceRecord("chr13", 115169878),
    SequenceRecord("chr3", 198022430),
    SequenceRecord("chr6", 171115067),
    SequenceRecord("chr9", 141213431),
    SequenceRecord("chr16", 90354753),
    SequenceRecord("chr10", 135534747),
    SequenceRecord("chr12", 133851895),
    SequenceRecord("chr8", 146364022),
    SequenceRecord("chr19", 59128983),
    SequenceRecord("chr2", 243199373),
    SequenceRecord("chr15", 102531392),
    SequenceRecord("chr14", 107349540),
    SequenceRecord("chr17", 81195210),
    SequenceRecord("chr5", 180915260),
    SequenceRecord("chr4", 191154276),
    SequenceRecord("chr1", 249250621),
    SequenceRecord("chr21", 48129895),
    SequenceRecord("chr11,", 135006516)
  ))

  sparkTest("testing that partition and sort provide correct outputs") {
    // load in a generic bam
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)

    // sort and make into 16 partitions
    val y = x.sortLexicographically(storePartitionMap = true, partitions = 16)

    // sort and make into 32 partitions
    val z = x.sortLexicographically(storePartitionMap = true, partitions = 32)
    val arrayRepresentationOfZ = z.rdd.collect

    //verify sort worked on actual values
    for (currentArray <- List(y.rdd.collect, z.rdd.collect)) {
      for (i <- currentArray.indices) {
        if (i != 0) assert(
          ReferenceRegion(arrayRepresentationOfZ(i).getReferenceName,
            arrayRepresentationOfZ(i).getStart,
            arrayRepresentationOfZ(i).getEnd).compareTo(
              ReferenceRegion(arrayRepresentationOfZ(i - 1).getReferenceName,
                arrayRepresentationOfZ(i - 1).getStart,
                arrayRepresentationOfZ(i - 1).getEnd)) >= 0)
      }
    }

    val partitionTupleCounts: Array[Int] = z.rdd.mapPartitions(f => Iterator(f.size)).collect
    val partitionTupleCounts2: Array[Int] = y.rdd.mapPartitions(f => Iterator(f.size)).collect
    // make sure that we didn't lose any data
    assert(partitionTupleCounts.sum == partitionTupleCounts2.sum)
  }

  sparkTest("testing copartition maintains or adds sort") {
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)
    val z = x.sortLexicographically(storePartitionMap = true, partitions = 16)
    val y = x.sortLexicographically(storePartitionMap = true, partitions = 32)
    val a = x.copartitionByReferenceRegion(y)
    val b = z.copartitionByReferenceRegion(y)

    assert(!a.rdd.zip(b.rdd).collect.exists(f => f._1 != f._2))
  }

  sparkTest("testing that we don't drop any data on the right side even though it doesn't map to a partition on the left") {
    // testing the left side with an extremely large region that is
    // not the last record on a partition
    // this test also tests the case that our
    val genotypeBuilder = new ListBuffer[Genotype]()

    genotypeBuilder += {
      Genotype.newBuilder()
        .setReferenceName("chr1")
        .setStart(2L)
        .setEnd(100L)
        .setVariant(
          Variant.newBuilder()
            .setStart(2L)
            .setEnd(100L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("1")
        .build()
    }

    genotypeBuilder += {
      Genotype.newBuilder()
        .setReferenceName("chr1")
        .setStart(3L)
        .setEnd(5L)
        .setVariant(
          Variant.newBuilder()
            .setStart(3L)
            .setEnd(5L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("2")
        .build()
    }

    genotypeBuilder += {
      Genotype.newBuilder()
        .setReferenceName("chr1")
        .setStart(6L)
        .setEnd(7L)
        .setVariant(
          Variant.newBuilder()
            .setStart(6L)
            .setEnd(7L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("3")
        .build()
    }

    genotypeBuilder += {
      Genotype.newBuilder()
        .setReferenceName("chr1")
        .setStart(8L)
        .setEnd(12L)
        .setVariant(
          Variant.newBuilder()
            .setStart(8L)
            .setEnd(12L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("3")
        .build()
    }

    val featureBuilder = new ListBuffer[Feature]()

    featureBuilder += {
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(61L)
        .setEnd(62L)
        .build()
    }

    featureBuilder += {
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(11L)
        .setEnd(15L)
        .build()
    }

    featureBuilder += {
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(3L)
        .setEnd(6L)
        .build()
    }

    featureBuilder += {
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(6L)
        .setEnd(8L)
        .build()
    }

    featureBuilder += {
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(50L)
        .setEnd(52L)
        .build()
    }

    featureBuilder += {
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(1L)
        .setEnd(2L)
        .build()
    }

    val genotypes =
      GenotypeDataset(sc.parallelize(genotypeBuilder),
        sd, Seq(), DefaultHeaderLines.allHeaderLines)
        .sortLexicographically(storePartitionMap = true, partitions = 2)
    genotypes.rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => (idx, f))
    }).collect
    val features = FeatureDataset(sc.parallelize(featureBuilder), sd, Seq.empty)
    val x = features.copartitionByReferenceRegion(genotypes)
    val z = x.rdd.mapPartitionsWithIndex((idx, iter) => {
      if (idx == 0 && iter.size != 6) {
        Iterator(true)
      } else if (idx == 1 && iter.size != 4) {
        Iterator(true)
      } else {
        Iterator()
      }
    })

    x.rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => (idx, f))
    }).collect
    assert(z.collect.length == 0)

  }

  sparkTest("testing that sorted shuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)
    // sort and make into 16 partitions
    val z =
      x.sortLexicographically(storePartitionMap = true, partitions = 1600)

    // perform join using 1600 partitions
    // 1600 is much more than the amount of data in the GenomicDataset
    // so we also test our ability to handle this extreme request
    val b = z.shuffleRegionJoin(x, Some(1600), 0L)
    val c = x.shuffleRegionJoin(z, Some(1600), 0L)
    val d = c.rdd.map(f => (f._1.getStart, f._2.getEnd)).collect.toSet
    val e = b.rdd.map(f => (f._1.getStart, f._2.getEnd)).collect.toSet

    val setDiff = d -- e
    assert(setDiff.isEmpty)

    assert(b.rdd.count == c.rdd.count)
  }

  sparkTest("testing that sorted fullOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)
    val z = x.sortLexicographically(storePartitionMap = true, partitions = 16)
    val d = x.fullOuterShuffleRegionJoin(z, Some(1), 0L)
    val e = z.fullOuterShuffleRegionJoin(x, Some(1), 0L)

    val setDiff = d.rdd.collect.toSet -- e.rdd.collect.toSet
    assert(setDiff.isEmpty)
    assert(d.rdd.count == e.rdd.count)
  }

  sparkTest("testing that sorted rightOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)
    val z = x.sortLexicographically(storePartitionMap = true, partitions = 1)
    val f = z.rightOuterShuffleRegionJoin(x, Some(1), 0L).rdd.collect
    val g = x.rightOuterShuffleRegionJoin(x).rdd.collect

    val setDiff = f.toSet -- g.toSet
    assert(setDiff.isEmpty)
    assert(f.length == g.length)
  }

  sparkTest("testing that sorted leftOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)
    val z = x.sortLexicographically(storePartitionMap = true, partitions = 1)
    val h = z.leftOuterShuffleRegionJoin(x, Some(1), 0L).rdd
    val i = z.leftOuterShuffleRegionJoin(x).rdd

    val setDiff = h.collect.toSet -- i.collect.toSet
    assert(setDiff.isEmpty)
    assert(h.count == i.count)
  }

  sparkTest("testing that we can persist the sorted knowledge") {
    val x = sc.loadBam(resourceUrl("reads12.sam").getFile)
    val z = x.sortLexicographically(storePartitionMap = true, partitions = 4)
    val fileLocation = tmpLocation()
    val saveArgs = new JavaSaveArgs(fileLocation, asSingleFile = false)
    z.save(saveArgs, isSorted = true)

    val t = sc.loadParquetAlignments(fileLocation)
    assert(t.isSorted)
    assert(t.rdd.partitions.length == z.rdd.partitions.length)
    // determine that our data still fits within the partition map
    assert(!t.rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => (idx, f))
    }).zip(z.rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => (idx, f))
    })).collect.exists(f => f._1 != f._2))

    val test = t.rdd.collect.drop(1)
    val test2 = t.rdd.collect.dropRight(1)
    assert(!test2.zip(test).exists(f => {
      ReferenceRegion(f._1.getReferenceName, f._1.getStart, f._1.getEnd)
        .compareTo(ReferenceRegion(f._2.getReferenceName, f._2.getStart, f._2.getEnd)) >= 0
    }))
  }
}
