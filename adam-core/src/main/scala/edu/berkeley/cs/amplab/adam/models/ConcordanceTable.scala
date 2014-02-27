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

package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.rich.GenotypeType

object ConcordanceTable {
  def apply() = new ConcordanceTable()

  def create(v: (GenotypeType, GenotypeType)) = new ConcordanceTable(v)

  /**
   *
   * @param l Modified
   * @param r
   * @return
   */

  def addComparison(l: ConcordanceTable, r: (GenotypeType, GenotypeType)) = {
    l.add(r)
    l
  }

  /**
   *
   * @param l Modified
   * @param r
   * @return
   */
  def mergeTable(l: ConcordanceTable, r: ConcordanceTable) = {
    l.add(r)
    l
  }

  // Relevant sub-groups of concordance table entries
  val CALLED  = Seq(GenotypeType.HOM_REF, GenotypeType.HET, GenotypeType.HOM_ALT)
  val VARIANT = Seq(GenotypeType.HET, GenotypeType.HOM_ALT)
  val ALL     = GenotypeType.values.toSeq

  implicit def typesToIndices(t : Seq[GenotypeType]) : Seq[Int] = t.map(_.ordinal)
}


class ConcordanceTable {
  import ConcordanceTable._

  private val table_ = Array.fill[Long](GenotypeType.values.length, GenotypeType.values.length)(0L)

  def this(p : (GenotypeType, GenotypeType)) = {
    this()
    add(p)
  }

  def add(p : (GenotypeType, GenotypeType)) = table_(p._1.ordinal)(p._2.ordinal) += 1L
  def add(that : ConcordanceTable) = {
    for (r <- ALL : Seq[Int]; c <- ALL : Seq[Int])
      table_(r)(c) += that.table_(r)(c)
  }

  def get(test: GenotypeType, truth: GenotypeType) : Long = table_(test.ordinal)(truth.ordinal)


  def total(diagonal : Seq[Int]) = ( for (i <- diagonal) yield table_(i)(i) ).sum
  def total(test : Seq[Int] = ALL, truth : Seq[Int] = ALL) =
    ( for (r <- test; c <- truth) yield table_(r)(c) ).sum

  def concordance : Double = total(CALLED).toDouble / total(CALLED, CALLED).toDouble
  def nonReferenceSensitivity : Double = total(VARIANT, VARIANT).toDouble / total(ALL, VARIANT).toDouble
}
