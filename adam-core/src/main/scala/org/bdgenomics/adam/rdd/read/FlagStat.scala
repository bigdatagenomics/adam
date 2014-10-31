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
package org.bdgenomics.adam.rdd.read

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.Util._
import org.bdgenomics.formats.avro.AlignmentRecord

object FlagStatMetrics {
  val emptyFailedQuality = new FlagStatMetrics(0, DuplicateMetrics.empty, DuplicateMetrics.empty, 0, 0, 0, 0, 0, 0, 0, 0, 0, true)
  val emptyPassedQuality = new FlagStatMetrics(0, DuplicateMetrics.empty, DuplicateMetrics.empty, 0, 0, 0, 0, 0, 0, 0, 0, 0, false)
}

object DuplicateMetrics {
  val empty = new DuplicateMetrics(0, 0, 0, 0)

  def apply(record: AlignmentRecord): (DuplicateMetrics, DuplicateMetrics) = {
    import org.bdgenomics.adam.rdd.read.FlagStat.b2i

    def isPrimary(record: AlignmentRecord): Boolean = {
      record.getDuplicateRead && record.getPrimaryAlignment
    }
    def isSecondary(record: AlignmentRecord): Boolean = {
      record.getDuplicateRead && !record.getPrimaryAlignment
    }

    def duplicateMetrics(f: (AlignmentRecord) => Boolean) = {
      new DuplicateMetrics(b2i(f(record)),
        b2i(f(record) && record.getReadMapped && record.getMateMapped),
        b2i(f(record) && record.getReadMapped && !record.getMateMapped),
        b2i(f(record) && (!isSameContig(record.getContig, record.getMateContig))))
    }
    (duplicateMetrics(isPrimary), duplicateMetrics(isSecondary))
  }
}

case class DuplicateMetrics(total: Long, bothMapped: Long, onlyReadMapped: Long, crossChromosome: Long) {
  def +(that: DuplicateMetrics): DuplicateMetrics = {
    new DuplicateMetrics(total + that.total,
      bothMapped + that.bothMapped,
      onlyReadMapped + that.onlyReadMapped,
      crossChromosome + that.crossChromosome)
  }
}

case class FlagStatMetrics(total: Long, duplicatesPrimary: DuplicateMetrics, duplicatesSecondary: DuplicateMetrics,
                           mapped: Long, pairedInSequencing: Long,
                           read1: Long, read2: Long, properlyPaired: Long, withSelfAndMateMapped: Long,
                           singleton: Long, withMateMappedToDiffChromosome: Long,
                           withMateMappedToDiffChromosomeMapQ5: Long, failedQuality: Boolean) {
  def +(that: FlagStatMetrics): FlagStatMetrics = {
    assert(failedQuality == that.failedQuality, "Can't reduce passedVendorQuality with different failedQuality values")
    new FlagStatMetrics(total + that.total,
      duplicatesPrimary + that.duplicatesPrimary,
      duplicatesSecondary + that.duplicatesSecondary,
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

  def b2i(boolean: Boolean) = if (boolean) 1 else 0

  def apply(rdd: RDD[AlignmentRecord]) = {
    rdd.map {
      p =>
        val mateMappedToDiffChromosome =
          p.getReadPaired && p.getReadMapped && p.getMateMapped && !isSameContig(p.getContig, p.getMateContig)
        val (primaryDuplicates, secondaryDuplicates) = DuplicateMetrics(p)
        new FlagStatMetrics(1,
          primaryDuplicates, secondaryDuplicates,
          b2i(p.getReadMapped),
          b2i(p.getReadPaired),
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
