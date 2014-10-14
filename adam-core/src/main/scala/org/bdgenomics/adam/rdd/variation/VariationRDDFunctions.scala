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
package org.bdgenomics.adam.rdd.variation

import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  VariantContext,
  SequenceRecord
}
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.rich.RichGenotype._
import org.bdgenomics.formats.avro.{ Genotype, GenotypeType, DatabaseVariantAnnotation }

class VariantContextRDDFunctions(rdd: RDD[VariantContext]) extends ADAMSequenceDictionaryRDDAggregator[VariantContext](rdd) {

  /**
   * For a single variant context, returns sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecordsFromElement(elem: VariantContext): scala.collection.Set[SequenceRecord] = {
    elem.genotypes.map(gt => SequenceRecord.fromSpecificRecord(gt.getVariant)).toSet
  }

  /**
   * Left outer join database variant annotations
   *
   */
  def joinDatabaseVariantAnnotation(ann: RDD[DatabaseVariantAnnotation]): RDD[VariantContext] = {
    rdd.keyBy(_.variant)
      .leftOuterJoin(ann.keyBy(_.getVariant))
      .values
      .map { case (v: VariantContext, a) => VariantContext(v.variant, v.genotypes, a) }

  }

  def getCallsetSamples(): List[String] = {
    rdd.flatMap(c => c.genotypes.map(_.getSampleId).toSeq.distinct)
      .distinct
      .map(_.toString)
      .collect()
      .toList
  }
}

class GenotypeRDDFunctions(rdd: RDD[Genotype]) extends Serializable with Logging {
  def toVariantContext(): RDD[VariantContext] = {
    rdd.keyBy({ g => RichVariant.variantToRichVariant(g.getVariant) })
      .groupByKey
      .map { case (v: RichVariant, g) => new VariantContext(v, g, None) }
  }

  /**
   * Compute the per-sample ConcordanceTable for this genotypes vs. the supplied
   * truth dataset. Only genotypes with ploidy <= 2 will be considered.
   * @param truth Truth genotypes
   * @return PairedRDD of sample -> ConcordanceTable
   */
  def concordanceWith(truth: RDD[Genotype]): RDD[(String, ConcordanceTable)] = {
    // Concordance approach only works for ploidy <= 2, e.g. diploid/haploid
    val keyedTest = rdd.filter(_.ploidy <= 2)
      .keyBy(g => (g.getVariant, g.getSampleId.toString): (RichVariant, String))
    val keyedTruth = truth.filter(_.ploidy <= 2)
      .keyBy(g => (g.getVariant, g.getSampleId.toString): (RichVariant, String))

    val inTest = keyedTest.leftOuterJoin(keyedTruth)
    val justInTruth = keyedTruth.subtractByKey(inTest)

    // Compute RDD[sample -> ConcordanceTable] across variants/samples
    val inTestPairs = inTest.map({
      case ((_, sample), (l, Some(r))) => sample -> (l.getType, r.getType)
      case ((_, sample), (l, None))    => sample -> (l.getType, GenotypeType.NO_CALL)
    })
    val justInTruthPairs = justInTruth.map({ // "truth-only" entries
      case ((_, sample), r) => sample -> (GenotypeType.NO_CALL, r.getType)
    })

    val bySample = inTestPairs.union(justInTruthPairs).combineByKey(
      (p: (GenotypeType, GenotypeType)) => ConcordanceTable(p),
      (l: ConcordanceTable, r: (GenotypeType, GenotypeType)) => l.add(r),
      (l: ConcordanceTable, r: ConcordanceTable) => l.add(r))

    bySample
  }
}
