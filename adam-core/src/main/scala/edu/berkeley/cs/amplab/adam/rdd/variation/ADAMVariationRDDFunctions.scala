/*
 * Copyright (c) 2013. Mount Sinai School of Medicine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.rdd.variation

import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotypeType, ADAMGenotype, ADAMDatabaseVariantAnnotation}
import edu.berkeley.cs.amplab.adam.models.{ADAMVariantContext,
                                           SequenceDictionary,
                                           SequenceRecord}
import edu.berkeley.cs.amplab.adam.rdd.AdamSequenceDictionaryRDDAggregator
import edu.berkeley.cs.amplab.adam.rich.RichADAMVariant
import edu.berkeley.cs.amplab.adam.rich.RichADAMGenotype._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class ADAMVariantContextRDDFunctions(rdd: RDD[ADAMVariantContext]) extends AdamSequenceDictionaryRDDAggregator[ADAMVariantContext](rdd) {

  /**
   * For a single variant context, returns sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecordsFromElement (elem: ADAMVariantContext): scala.collection.Set[SequenceRecord] = {
    elem.genotypes.map(gt => SequenceRecord.fromSpecificRecord(gt.getVariant)).toSet
  }

  /**
   * Left outer join database variant annotations
   *
   */
  def joinDatabaseVariantAnnotation(ann: RDD[ADAMDatabaseVariantAnnotation]): RDD[ADAMVariantContext] = {
    rdd.keyBy(_.variant)
      .leftOuterJoin(ann.keyBy(_.getVariant))
      .values
      .map { case (v:ADAMVariantContext, a) => ADAMVariantContext(v.variant, v.genotypes, a) }

  }

  def adamGetCallsetSamples(): List[String] = {
    rdd.flatMap(c => c.genotypes.map(_.getSampleId).distinct)
      .distinct
      .map(_.toString)
      .collect()
      .toList
  }
}

class ADAMGenotypeRDDFunctions(rdd: RDD[ADAMGenotype]) extends Serializable with Logging {
  def toADAMVariantContext(): RDD[ADAMVariantContext] = {
    rdd.keyBy({ g => RichADAMVariant.variantToRichVariant(g.getVariant) })
      .groupByKey
      .map { case (v:RichADAMVariant, g) => new ADAMVariantContext(v, g, None) }
  }

  /**
   * Compute the per-sample ConcordanceTable for this genotypes vs. the supplied
   * truth dataset. Only genotypes with ploidy <= 2 will be considered.
   * @param truth Truth genotypes
   * @return PairedRDD of sample -> ConcordanceTable
   */
  def concordanceWith(truth: RDD[ADAMGenotype]) : RDD[(String, ConcordanceTable)] = {
    // Concordance approach only works for ploidy <= 2, e.g. diploid/haploid
    val keyedTest  = rdd.filter(_.ploidy <= 2)
      .keyBy(g => (g.getVariant, g.getSampleId.toString) : (RichADAMVariant, String))
    val keyedTruth = truth.filter(_.ploidy <= 2)
      .keyBy(g => (g.getVariant, g.getSampleId.toString) : (RichADAMVariant, String))

    val inTest = keyedTest.leftOuterJoin(keyedTruth)
    val justInTruth = keyedTruth.subtractByKey(inTest)

    // Compute RDD[sample -> ConcordanceTable] across variants/samples
    val inTestPairs = inTest.map({
      case ((_, sample), (l, Some(r))) => sample -> (l.getType, r.getType)
      case ((_, sample), (l, None))    => sample -> (l.getType, ADAMGenotypeType.NO_CALL)
    })
    val justInTruthPairs = justInTruth.map({ // "truth-only" entries
      case ((_, sample), r) => sample -> (ADAMGenotypeType.NO_CALL, r.getType)
    })

    val bySample = inTestPairs.union(justInTruthPairs).combineByKey(
      (p : (ADAMGenotypeType, ADAMGenotypeType)) => ConcordanceTable(p),
      (l : ConcordanceTable, r : (ADAMGenotypeType, ADAMGenotypeType)) => l.add(r),
      (l : ConcordanceTable, r : ConcordanceTable) => l.add(r)
    )

    bySample
  }
}
