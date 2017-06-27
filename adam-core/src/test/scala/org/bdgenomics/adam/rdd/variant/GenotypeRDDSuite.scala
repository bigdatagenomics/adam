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

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ GenomicRDD, SortedGenomicRDD }
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Genotype

class GenotypeRDDSuite extends ADAMFunSuite {

  sparkTest("union two genotype rdds together") {
    val genotype1 = sc.loadGenotypes(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
    val genotype2 = sc.loadGenotypes(testFile("small.vcf"))
    val union = genotype1.union(genotype2)
    assert(union.rdd.count === (genotype1.rdd.count + genotype2.rdd.count))
    assert(union.sequences.size === (genotype1.sequences.size + genotype2.sequences.size))
    assert(union.samples.size === 4)
  }

  sparkTest("use broadcast join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = genotypes.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 9L)
  }

  sparkTest("use right outer broadcast join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = genotypes.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 9)
  }

  sparkTest("use shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    // rdd is too small for sampling rate of 0.1 to reliably work
    sc.hadoopConfiguration.setDouble(GenomicRDD.FLANK_SAMPLING_PERCENT, 1.0)

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.shuffleRegionJoin(targets)
    val jRdd0 = genotypes.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 9L)
    assert(jRdd0.rdd.count === 9L)
  }

  sparkTest("use right outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    // rdd is too small for sampling rate of 0.1 to reliably work
    sc.hadoopConfiguration.setDouble(GenomicRDD.FLANK_SAMPLING_PERCENT, 1.0)

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 9)
    assert(c0.count(_._1.isDefined) === 9)
  }

  sparkTest("use left outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    // rdd is too small for sampling rate of 0.1 to reliably work
    sc.hadoopConfiguration.setDouble(GenomicRDD.FLANK_SAMPLING_PERCENT, 1.0)

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 9)
    assert(c0.count(_._2.isEmpty) === 9)
    assert(c.count(_._2.isDefined) === 9)
    assert(c0.count(_._2.isDefined) === 9)
  }

  sparkTest("use full outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    // rdd is too small for sampling rate of 0.1 to reliably work
    sc.hadoopConfiguration.setDouble(GenomicRDD.FLANK_SAMPLING_PERCENT, 1.0)

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 9)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 9)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 9)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 9)
  }

  sparkTest("use shuffle join with group by to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    // rdd is too small for sampling rate of 0.1 to reliably work
    sc.hadoopConfiguration.setDouble(GenomicRDD.FLANK_SAMPLING_PERCENT, 1.0)

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = genotypes.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 9)
    assert(c0.size === 9)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    // rdd is too small for sampling rate of 0.1 to reliably work
    sc.hadoopConfiguration.setDouble(GenomicRDD.FLANK_SAMPLING_PERCENT, 1.0)

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = genotypes.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect

    assert(c.count(_._1.isDefined) === 18)
    assert(c0.count(_._1.isDefined) === 18)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 9)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 9)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 9)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 9)
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  sparkTest("convert back to variant contexts") {
    val genotypesPath = testFile("small.vcf")
    val genotypes = sc.loadGenotypes(genotypesPath)
    val variantContexts = genotypes.toVariantContextRDD

    assert(variantContexts.sequences.containsRefName("1"))
    assert(variantContexts.samples.nonEmpty)

    val vcs = variantContexts.rdd.collect
    assert(vcs.size === 6)

    val vc = vcs.head
    assert(vc.position.referenceName === "1")
    assert(vc.variant.variant.contigName === "1")
    assert(vc.genotypes.nonEmpty)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    val inputPath = testFile("small.vcf")
    val outputPath = tmpLocation()
    val vcfRdd = sc.loadGenotypes(inputPath)
    assert(vcfRdd match {
      case _: SortedGenomicRDD[Genotype, GenotypeRDD] => {
        true
      }
      case _ => {
        false
      }
    })

    val rdd = vcfRdd.transformDataset(ds => ds) // no-op but force to sql
    assert(rdd.dataset.count === 18)
    assert(rdd.rdd.count === 18)
    rdd.saveAsParquet(outputPath)
    val rdd2 = sc.loadGenotypes(outputPath)
    assert(rdd2.rdd.count === 18)
    assert(rdd2.dataset.count === 18)
    val outputPath2 = tmpLocation()
    rdd.sort()
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadGenotypes(outputPath2)
    assert(rdd3 match {
      case _: SortedGenomicRDD[Genotype, GenotypeRDD] => {
        true
      }
      case _ => {
        false
      }
    })
    assert(rdd3.rdd.count === 18)
    assert(rdd3.dataset.count === 18)
  }
}
