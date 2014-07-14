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
package org.bdgenomics.adam.parquet_reimpl.index

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.ADAMFlatGenotype
import scala.math._

/**
 * ReferenceFolder is a trait for folding a value (of type T) into a Sequence of ReferenceRegion values.
 *
 * This is primarily used, right now, in the index building for the IDRangeIndex, where we find
 * contiguous regions of the genome corresponding to each Parquet row group by accumulating variant
 * positions that are within a certain 'window' distance of each other.
 *
 * We _could_ have avoided writing ReferenceFolder altogether, and just used the existing ReferenceMapping
 * trait and implicit conversions for ADAMRecord, ADAMFlatGenotype, etc.  However, that necessitates the
 * creation of a few additional values per comparison, and that extra object creation was putting pressure
 * on our GC times. ReferenceFolder is, therefore, ultimately an optimization for performance.
 *
 * @tparam T the type of the values to fold -- should be something that is located along the genome.
 */
trait ReferenceFolder[T] extends Serializable {
  def fold(regions: Seq[ReferenceRegion], value: T): Seq[ReferenceRegion]
}

class ADAMFlatGenotypeReferenceFolder(val rangeGapSize: Long = 10000L) extends ReferenceFolder[ADAMFlatGenotype] {

  var count: Long = 0L

  def distance(r: ReferenceRegion, pos: Long): Long =
    if (pos < r.start) r.start - pos
    else if (pos > r.end) pos - r.end + 1
    else 0

  def canCombine(fg: ADAMFlatGenotype, r: ReferenceRegion): Boolean =
    r.referenceName == fg.getReferenceName && distance(r, fg.getPosition) <= rangeGapSize

  def combine(fg: ADAMFlatGenotype, r: ReferenceRegion): ReferenceRegion =
    if (r.start <= fg.getPosition && r.end > fg.getPosition) r
    else ReferenceRegion(r.referenceName, min(r.start, fg.getPosition), max(r.end, fg.getPosition + 1))

  def lift(fg: ADAMFlatGenotype): ReferenceRegion =
    ReferenceRegion(fg.getReferenceName.toString, fg.getPosition, fg.getPosition + 1)

  override def fold(regions: Seq[ReferenceRegion], value: ADAMFlatGenotype): Seq[ReferenceRegion] = {
    count += 1
    if (count % 10000 == 0) println("Processed %dk records".format(count / 1000))

    regions match {
      case Seq() => Seq(lift(value))
      case array: Seq[ReferenceRegion] =>
        val lastElement = array(array.length - 1)
        if (canCombine(value, lastElement))
          array.slice(0, array.length - 1) :+ combine(value, lastElement)
        else
          array :+ lift(value)
    }
  }
}

object ReferenceFoldingContext {
  implicit val adamFlatGenotypeReferenceFolder = new ADAMFlatGenotypeReferenceFolder(10000L)
}
