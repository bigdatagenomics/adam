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
package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{ Projection, AlignmentRecordField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.kohsuke.args4j.Argument

object FlagStat extends ADAMCommandCompanion {
  val commandName: String = "flagstat"
  val commandDescription: String = "Print statistics on reads in an ADAM file (similar to samtools flagstat)"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new FlagStat(Args4j[FlagStatArgs](cmdLine))
  }
}

class FlagStatArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM data to return stats for", index = 0)
  val inputPath: String = null
}

class FlagStat(protected val args: FlagStatArgs) extends ADAMSparkCommand[FlagStatArgs] {
  val companion: ADAMCommandCompanion = FlagStat

  def run(sc: SparkContext, job: Job): Unit = {

    val projection = Projection(
      AlignmentRecordField.readMapped,
      AlignmentRecordField.mateMapped,
      AlignmentRecordField.readPaired,
      AlignmentRecordField.contig,
      AlignmentRecordField.mateContig,
      AlignmentRecordField.primaryAlignment,
      AlignmentRecordField.duplicateRead,
      AlignmentRecordField.readMapped,
      AlignmentRecordField.mateMapped,
      AlignmentRecordField.firstOfPair,
      AlignmentRecordField.secondOfPair,
      AlignmentRecordField.properPair,
      AlignmentRecordField.mapq,
      AlignmentRecordField.failedVendorQualityChecks)

    val adamFile: RDD[AlignmentRecord] = sc.loadAlignments(args.inputPath, projection = Some(projection))

    val (failedVendorQuality, passedVendorQuality) = adamFile.adamFlagStat()

    def percent(fraction: Long, total: Long) = if (total == 0) 0.0 else 100.00 * fraction.toFloat / total

    println("""
               |%d + %d in total (QC-passed reads + QC-failed reads)
               |%d + %d primary duplicates
               |%d + %d primary duplicates - both read and mate mapped
               |%d + %d primary duplicates - only read mapped
               |%d + %d primary duplicates - cross chromosome
               |%d + %d secondary duplicates
               |%d + %d secondary duplicates - both read and mate mapped
               |%d + %d secondary duplicates - only read mapped
               |%d + %d secondary duplicates - cross chromosome
               |%d + %d mapped (%.2f%%:%.2f%%)
               |%d + %d paired in sequencing
               |%d + %d read1
               |%d + %d read2
               |%d + %d properly paired (%.2f%%:%.2f%%)
               |%d + %d with itself and mate mapped
               |%d + %d singletons (%.2f%%:%.2f%%)
               |%d + %d with mate mapped to a different chr
               |%d + %d with mate mapped to a different chr (mapQ>=5)
             """.stripMargin('|').format(
      passedVendorQuality.total, failedVendorQuality.total,
      passedVendorQuality.duplicatesPrimary.total, failedVendorQuality.duplicatesPrimary.total,
      passedVendorQuality.duplicatesPrimary.bothMapped, failedVendorQuality.duplicatesPrimary.bothMapped,
      passedVendorQuality.duplicatesPrimary.onlyReadMapped, failedVendorQuality.duplicatesPrimary.onlyReadMapped,
      passedVendorQuality.duplicatesPrimary.crossChromosome, failedVendorQuality.duplicatesPrimary.crossChromosome,
      passedVendorQuality.duplicatesSecondary.total, failedVendorQuality.duplicatesSecondary.total,
      passedVendorQuality.duplicatesSecondary.bothMapped, failedVendorQuality.duplicatesSecondary.bothMapped,
      passedVendorQuality.duplicatesSecondary.onlyReadMapped, failedVendorQuality.duplicatesSecondary.onlyReadMapped,
      passedVendorQuality.duplicatesSecondary.crossChromosome, failedVendorQuality.duplicatesSecondary.crossChromosome,
      passedVendorQuality.mapped, failedVendorQuality.mapped,
      percent(passedVendorQuality.mapped, passedVendorQuality.total),
      percent(failedVendorQuality.mapped, failedVendorQuality.total),
      passedVendorQuality.pairedInSequencing, failedVendorQuality.pairedInSequencing,
      passedVendorQuality.read1, failedVendorQuality.read1,
      passedVendorQuality.read2, failedVendorQuality.read2,
      passedVendorQuality.properlyPaired, failedVendorQuality.properlyPaired,
      percent(passedVendorQuality.properlyPaired, passedVendorQuality.total),
      percent(failedVendorQuality.properlyPaired, failedVendorQuality.total),
      passedVendorQuality.withSelfAndMateMapped, failedVendorQuality.withSelfAndMateMapped,
      passedVendorQuality.singleton, failedVendorQuality.singleton,
      percent(passedVendorQuality.singleton, passedVendorQuality.total),
      percent(failedVendorQuality.singleton, failedVendorQuality.total),
      passedVendorQuality.withMateMappedToDiffChromosome, failedVendorQuality.withMateMappedToDiffChromosome,
      passedVendorQuality.withMateMappedToDiffChromosomeMapQ5, failedVendorQuality.withMateMappedToDiffChromosomeMapQ5))
  }

}
