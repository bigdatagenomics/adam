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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

class ADAM2FastqArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The read file to convert", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the FASTQ to", index = 1)
  var outputPath: String = null
  @Argument(required = false, metaVar = "SECOND_OUTPUT", usage = "When writing FASTQ data, all second-in-pair reads will go here, if this argument is provided", index = 2)
  var outputPath2: String = null
  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT and SECOND_OUTPUT as single files")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
  @Args4jOption(required = false, name = "-validation", usage = "SAM tools validation level; when STRICT, checks that all reads are paired.")
  var validationStringency = ValidationStringency.LENIENT
  @Args4jOption(required = false, name = "-repartition", usage = "Set the number of partitions to map data to")
  var repartition: Int = -1
  @Args4jOption(required = false, name = "-persist_level", usage = "Persist() intermediate RDDs")
  var persistLevel: String = null
  @Args4jOption(required = false, name = "-no_projection", usage = "Disable projection on records. No great reason to do this, but useful for testing / comparison.")
  var disableProjection: Boolean = false
  @Args4jOption(required = false, name = "-output_oq", usage = "Output the original sequencing quality scores")
  var outputOriginalBaseQualities = false
}

object ADAM2Fastq extends BDGCommandCompanion {
  override val commandName = "adam2fastq"
  override val commandDescription = "Convert BAM to FASTQ files"

  override def apply(cmdLine: Array[String]): ADAM2Fastq =
    new ADAM2Fastq(Args4j[ADAM2FastqArgs](cmdLine))
}

class ADAM2Fastq(val args: ADAM2FastqArgs) extends BDGSparkCommand[ADAM2FastqArgs] {
  override val companion = ADAM2Fastq

  override def run(sc: SparkContext): Unit = {

    val projectionOpt =
      if (!args.disableProjection)
        Some(
          Projection(
            AlignmentRecordField.readName,
            AlignmentRecordField.sequence,
            AlignmentRecordField.quality,
            AlignmentRecordField.readInFragment,
            AlignmentRecordField.originalQuality
          )
        )
      else
        None

    var reads = sc.loadAlignments(args.inputPath, optProjection = projectionOpt)

    if (args.repartition != -1) {
      log.info("Repartitioning reads to to '%d' partitions".format(args.repartition))
      reads = reads.transform(_.repartition(args.repartition))
    }

    reads.saveAsFastq(
      args.outputPath,
      Option(args.outputPath2),
      asSingleFile = args.asSingleFile,
      disableFastConcat = args.disableFastConcat,
      outputOriginalBaseQualities = args.outputOriginalBaseQualities,
      validationStringency = args.validationStringency,
      persistLevel = Option(args.persistLevel).map(StorageLevel.fromString(_))
    )
  }
}
