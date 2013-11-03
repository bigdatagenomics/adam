/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use file except in compliance with the License.
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
package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import spark.RDD

object FlagStatMetrics {
  val emptyFailedQuality = new FlagStatMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true)
  val emptyPassedQuality = new FlagStatMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false)
}

case class FlagStatMetrics(total: Long, duplicates: Long, mapped: Long, pairedInSequencing: Long,
                           read1: Long, read2: Long, properlyPaired: Long, withSelfAndMateMapped: Long,
                           singleton: Long, withMateMappedToDiffChromosome: Long,
                           withMateMappedToDiffChromosomeMapQ5: Long, failedQuality: Boolean) {
  def +(that: FlagStatMetrics): FlagStatMetrics = {
    assert(failedQuality == that.failedQuality, "Can't reduce passedVendorQuality with different failedQuality values")
    new FlagStatMetrics(total + that.total,
      duplicates + that.duplicates,
      mapped + that.mapped,
      pairedInSequencing + that.pairedInSequencing,
      read1 + that.read1,
      read2 + that.read2,
      properlyPaired + that.properlyPaired,
      withSelfAndMateMapped + that.withSelfAndMateMapped,
      singleton + that.singleton,
      withMateMappedToDiffChromosome + that.withMateMappedToDiffChromosome,
      withMateMappedToDiffChromosomeMapQ5 + that.withMateMappedToDiffChromosomeMapQ5,
      failedQuality)
  }
}

object FlagStat {

  def apply(rdd: RDD[ADAMRecord]) = {
    rdd.map {
      p =>
        val mateMappedToDiffChromosome = p.getReadPaired && p.getReadMapped && p.getMateMapped && (p.getReferenceId != p.getMateReferenceId)
        def b2i(boolean: Boolean) = if (boolean) 1 else 0
        new FlagStatMetrics(1,
          b2i(p.getDuplicateRead),
          b2i(p.getReadMapped),
          b2i(p.getReadPaired && p.getReadPaired),
          b2i(p.getReadPaired && p.getFirstOfPair),
          b2i(p.getReadPaired && p.getSecondOfPair),
          b2i(p.getReadPaired && p.getProperPair),
          b2i(p.getReadPaired && p.getReadMapped && p.getMateMapped),
          b2i(p.getReadPaired && p.getReadMapped && !p.getMateMapped),
          b2i(mateMappedToDiffChromosome),
          b2i(mateMappedToDiffChromosome && p.getMapq >= 5),
          p.getFailedVendorQualityChecks)
    }.aggregate((FlagStatMetrics.emptyFailedQuality, FlagStatMetrics.emptyPassedQuality))(
      seqOp = {
        (a, b) =>
          if (b.failedQuality) {
            (a._1 + b, a._2)
          } else {
            (a._1, a._2 + b)
          }
      },
      combOp = {
        (a, b) =>
          (a._1 + b._1, a._2 + b._2)
      })
  }
}
