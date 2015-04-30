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
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4JOption, Argument }

class Adam2FastqArgs extends ParquetLoadSaveArgs {
  @Argument(required = false, metaVar = "OUTPUT", usage = "When writing FASTQ data, all second-in-pair reads will go here, if this argument is provided", index = 2)
  var outputPath2: String = null
  @Args4JOption(required = false, name = "-validation", usage = "SAM tools validation level; when STRICT, checks that all reads are paired.")
  var validationStringency = ValidationStringency.LENIENT
  @Args4JOption(required = false, name = "-repartition", usage = "Set the number of partitions to map data to")
  var repartition: Int = -1
  @Args4JOption(required = false, name = "-persist-level", usage = "Persist() intermediate RDDs")
  var persistLevel: String = null
  @Args4JOption(required = false, name = "-no-projection", usage = "Disable projection on records. No great reason to do this, but useful for testing / comparison.")
  var disableProjection: Boolean = false
}

object Adam2Fastq extends BDGCommandCompanion {
  override val commandName = "adam2fastq"
  override val commandDescription = "Convert BAM to FASTQ files"

  override def apply(cmdLine: Array[String]): Adam2Fastq =
    new Adam2Fastq(Args4j[Adam2FastqArgs](cmdLine))
}

class Adam2Fastq(val args: Adam2FastqArgs) extends BDGSparkCommand[Adam2FastqArgs] {
  override val companion = Adam2Fastq

  override def run(sc: SparkContext): Unit = {

    val projectionOpt =
      if (!args.disableProjection)
        Some(
          Projection(
            AlignmentRecordField.readName,
            AlignmentRecordField.sequence,
            AlignmentRecordField.qual,
            AlignmentRecordField.firstOfPair,
            AlignmentRecordField.secondOfPair
          )
        )
      else
        None

    var reads: RDD[AlignmentRecord] = sc.loadAlignments(args.inputPath, projection = projectionOpt)

    if (args.repartition != -1) {
      log.info("Repartitioning reads to to '%d' partitions".format(args.repartition))
      reads = reads.repartition(args.repartition)
    }

    reads.adamSaveAsFastq(
      args.outputPath,
      Option(args.outputPath2),
      validationStringency = args.validationStringency,
      persistLevel = Option(args.persistLevel).map(StorageLevel.fromString(_))
    )
  }

}
