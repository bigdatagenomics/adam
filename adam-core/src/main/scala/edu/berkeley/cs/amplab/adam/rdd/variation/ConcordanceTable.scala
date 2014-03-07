/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
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

import java.util.EnumSet
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.avro.ADAMGenotypeType

object ConcordanceTable {
  def apply() = new ConcordanceTable()
  def apply(p : (ADAMGenotypeType, ADAMGenotypeType)) = (new ConcordanceTable()).add(p)

  // Relevant sub-groups of concordance table entries
  val CALLED  = EnumSet.of(ADAMGenotypeType.HOM_REF, ADAMGenotypeType.HET, ADAMGenotypeType.HOM_ALT)
  val VARIANT = EnumSet.of(ADAMGenotypeType.HET, ADAMGenotypeType.HOM_ALT)
  val ALL     = EnumSet.allOf(classOf[ADAMGenotypeType])

  implicit def typesToIdxs(types: EnumSet[ADAMGenotypeType]) : Set[Int] = {
    types.asScala.map(_.ordinal).toSet
  }
}

/**
 * Helper class for maintaining the genotype concordance table and computing the relevant
 * metrics. Table is indexed by genotype zygosity. Many of the metrics are based on the
 * [[http://gatkforums.broadinstitute.org/discussion/48/using-varianteval GATK GenotypeConcordance Walker]]
 * Table is organized as test vs. truth, i.e. rows correspond to "test" genotypes, columns
 * to "truth" genotypes.
 */
class ConcordanceTable {
  import ConcordanceTable._

  private val table_ = Array.fill[Long](ADAMGenotypeType.values.length, ADAMGenotypeType.values.length)(0L)

  /**
   * Add single genotype-genotype comparison into this table.
   * @param p Tuple of (test, truth) GenotypeType
   * @return this
   */
  def add(p : (ADAMGenotypeType, ADAMGenotypeType)) : ConcordanceTable = {
    table_(p._1.ordinal)(p._2.ordinal) += 1L
    this
  }

  /**
   * Add that ConcordanceTable into this table.
   * @param that ConcordanceTable
   * @return this
   */
  def add(that : ConcordanceTable) : ConcordanceTable = {
    for (r <- ALL; c <- ALL)
      table_(r)(c) += that.table_(r)(c)
    this
  }

  /**
   * Get single table entry at (test, truth)
   */
  def get(test: ADAMGenotypeType, truth: ADAMGenotypeType) : Long = table_(test.ordinal)(truth.ordinal)

  def total() : Long = total(ALL, ALL)
  def total(diagonal : EnumSet[ADAMGenotypeType]) : Long = {
    var t = 0L
    for (i <- diagonal)
      t += table_(i)(i)
    t
  }

  /**
   * Total of all entries indexed by the cartesian product of test and truth
   */
  def total(test : EnumSet[ADAMGenotypeType], truth : EnumSet[ADAMGenotypeType]) : Long = {
    var t = 0L
    for (r <- test; c <- truth)
      t += table_(r)(c)
    t
  }

  private def ratio(num : Long, dnm : Long) = if (dnm == 0) 0.0 else num.toDouble / dnm.toDouble

  /**
   * Overally genotype concordance, or the percentage of identical genotypes (including homozygous reference calls)
   */
  def concordance = ratio(total(CALLED), total(CALLED, CALLED))

  /**
   * Non-reference sensitivity or NRS is a site-level variant sensitivity metric.
   */
  def nonReferenceSensitivity = ratio(total(VARIANT, VARIANT), total(ALL, VARIANT))

  /**
   * Non-reference concordance or NRC is similar to NRS, but requires strict zygosity matching
   * in the numerator.
   */
  def nonReferenceConcordance = ratio(total(VARIANT), total(ALL, VARIANT))

  /**
   * Alias for nonReferenceConcordance
   */
  def recall = nonReferenceConcordance

  /**
   * Non-reference discrepancy is a measure of discrepant calls, excluding matching
   * homozygous reference genotypes, which are easier to call.
   */
  def nonReferenceDiscrepancy = {
    val all_called = total(ALL, ALL)
    ratio(all_called - total(ALL), all_called - get(ADAMGenotypeType.HOM_REF, ADAMGenotypeType.HOM_REF))
  }

  /**
   * Precision metric. This metric is similar to NRC but with "test" and "truth" reversed.
   */
  def precision = ratio(total(VARIANT), total(VARIANT, ALL))
}
