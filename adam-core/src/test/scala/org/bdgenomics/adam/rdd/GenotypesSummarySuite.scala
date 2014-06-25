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
package org.bdgenomics.adam.rdd

import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.GenotypesSummaryCounts.ReferenceAndAlternate
import org.bdgenomics.adam.util.SparkFunSuite

class GenotypesSummarySuite extends SparkFunSuite {

  private def homRef = List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)
  private def het = List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Ref)
  private def homAlt = List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)
  private def noCall = List(ADAMGenotypeAllele.NoCall, ADAMGenotypeAllele.NoCall)

  private def variant(reference: String, alternate: String, position: Int): ADAMVariant = {
    ADAMVariant.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setPosition(position)
      .setReferenceAllele(reference)
      .setVariantAllele(alternate)
      .build
  }

  private def genotype(sample: String, variant: ADAMVariant, alleles: List[ADAMGenotypeAllele]) = {
    ADAMGenotype.newBuilder()
      .setSampleId(sample)
      .setVariant(variant)
      .setAlleles(alleles)
  }

  private def summarize(genotypes: Seq[ADAMGenotype]): GenotypesSummary = {
    val rdd = sc.parallelize(genotypes)
    GenotypesSummary(rdd)
  }

  sparkTest("simple genotypes summary") {
    val genotypes = List(
      genotype("alice", variant("A", "TT", 2), het).build,
      genotype("alice", variant("G", "A", 4), het).build,
      genotype("alice", variant("G", "C", 1), homRef).build,
      genotype("alice", variant("G", "T", 0), homAlt).build,
      genotype("alice", variant("GGG", "T", 7), het).build,
      genotype("alice", variant("T", "AA", 9), het).build,
      genotype("alice", variant("TT", "AA", 12), het).build,
      genotype("bob", variant("A", "TT", 2), het).build,
      genotype("bob", variant("A", "T", 3), het).build,
      genotype("bob", variant("A", "T", 9), het).build,
      genotype("bob", variant("T", "C", 4), het).setIsPhased(true).build,
      genotype("bob", variant("T", "A", 7), homRef).build,
      genotype("bob", variant("T", "G", 8), homAlt).build,
      genotype("bob", variant("T", "G", 12), noCall).build,
      genotype("empty", variant("T", "G", 12), noCall).build)

    val stats = summarize(genotypes)
    assert(stats.perSampleStatistics.size == 3)

    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCount == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("A", "T")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "G")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("C", "T")) == 0)
    assert(stats.perSampleStatistics("empty").insertionCount == 0)
    assert(stats.perSampleStatistics("empty").noCallCount == 1)
    assert(stats.perSampleStatistics("empty").phasedCount == 0)

    assert(stats.perSampleStatistics("alice").singleNucleotideVariantCount == 2)
    assert(stats.perSampleStatistics("alice").singleNucleotideVariantCounts(ReferenceAndAlternate("G", "A")) == 1)
    assert(stats.perSampleStatistics("alice").singleNucleotideVariantCounts(ReferenceAndAlternate("G", "C")) == 0)
    assert(stats.perSampleStatistics("alice").singleNucleotideVariantCounts(ReferenceAndAlternate("G", "T")) == 1)
    assert(stats.perSampleStatistics("alice").singleNucleotideVariantCounts(ReferenceAndAlternate("A", "T")) == 0)
    assert(stats.perSampleStatistics("alice").deletionCount == 1)
    assert(stats.perSampleStatistics("alice").insertionCount == 2)
    assert(stats.perSampleStatistics("alice").multipleNucleotideVariantCount == 1)
    assert(stats.perSampleStatistics("alice").phasedCount == 0)
    assert(stats.perSampleStatistics("alice").noCallCount == 0)
    assert(stats.perSampleStatistics("bob").singleNucleotideVariantCount == 4)
    assert(stats.perSampleStatistics("bob").singleNucleotideVariantCounts(ReferenceAndAlternate("A", "T")) == 2)
    assert(stats.perSampleStatistics("bob").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 1)
    assert(stats.perSampleStatistics("bob").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 1)
    assert(stats.perSampleStatistics("bob").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "G")) == 1)
    assert(stats.perSampleStatistics("bob").singleNucleotideVariantCounts(ReferenceAndAlternate("C", "T")) == 0)
    assert(stats.perSampleStatistics("bob").insertionCount == 1)
    assert(stats.perSampleStatistics("bob").noCallCount == 1)
    assert(stats.perSampleStatistics("bob").phasedCount == 1)

    assert(stats.aggregateStatistics.singleNucleotideVariantCount == 6)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("G", "A")) == 1)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("G", "C")) == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("G", "T")) == 1)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("A", "T")) == 2)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 1)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("T", "G")) == 1)
    assert(stats.aggregateStatistics.insertionCount == 3)
    assert(stats.aggregateStatistics.deletionCount == 1)
    assert(stats.aggregateStatistics.multipleNucleotideVariantCount == 1)

    assert(stats.singletonCount == 9)
    assert(stats.distinctVariantCount == 10)

    // Test that the formatting functions do not throw.
    GenotypesSummaryFormatting.format_csv(stats)
    GenotypesSummaryFormatting.format_human_readable(stats)
  }

  sparkTest("empty genotypes summary") {
    val genotypes = List(
      genotype("empty", variant("T", "G", 12), noCall).build)

    val stats = summarize(genotypes)

    assert(stats.perSampleStatistics.size == 1)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCount == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("A", "T")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("T", "G")) == 0)
    assert(stats.perSampleStatistics("empty").singleNucleotideVariantCounts(ReferenceAndAlternate("C", "T")) == 0)
    assert(stats.perSampleStatistics("empty").insertionCount == 0)
    assert(stats.perSampleStatistics("empty").noCallCount == 1)
    assert(stats.perSampleStatistics("empty").phasedCount == 0)

    assert(stats.aggregateStatistics.singleNucleotideVariantCount == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("G", "A")) == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("G", "C")) == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("G", "T")) == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("A", "T")) == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("T", "C")) == 0)
    assert(stats.aggregateStatistics.singleNucleotideVariantCounts(ReferenceAndAlternate("T", "G")) == 0)
    assert(stats.aggregateStatistics.insertionCount == 0)
    assert(stats.aggregateStatistics.deletionCount == 0)
    assert(stats.aggregateStatistics.multipleNucleotideVariantCount == 0)

    assert(stats.singletonCount == 0)
    assert(stats.distinctVariantCount == 0)

    // Test that the formatting functions do not throw.
    GenotypesSummaryFormatting.format_csv(stats)
    GenotypesSummaryFormatting.format_human_readable(stats)
  }
}
