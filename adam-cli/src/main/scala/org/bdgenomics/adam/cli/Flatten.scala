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

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.Flattener
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.HadoopUtil
import org.kohsuke.args4j.Argument
import parquet.avro.AvroParquetInputFormat
import parquet.hadoop.util.ContextUtil

object Flatten extends BDGCommandCompanion {
  val commandName = "flatten"
  val commandDescription = "Convert a ADAM format file to a version with a flattened " +
    "schema, suitable for querying with tools like Impala"

  def apply(cmdLine: Array[String]) = {
    new Flatten(Args4j[FlattenArgs](cmdLine))
  }
}

class FlattenArgs extends Args4jBase with ParquetSaveArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to flatten",
    index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the " +
    "flattened file to", index = 1)
  var outputPath: String = null
}

class Flatten(val args: FlattenArgs) extends BDGSparkCommand[FlattenArgs] with Logging {
  val companion = Flatten

  def run(sc: SparkContext) {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      args.inputPath,
      classOf[AvroParquetInputFormat],
      classOf[Void],
      classOf[IndexedRecord],
      ContextUtil.getConfiguration(job)
    )
    if (Metrics.isRecording) records.instrument() else records

    val schema: Schema = records.first()._2.getSchema
    val flatSchema = Flattener.flattenSchema(schema)
    val flatSchemaBc = sc.broadcast(flatSchema) // broadcast since Avro schema not serializable

    records.map(r => Flattener.flattenRecord(flatSchemaBc.value, r._2))
      .adamParquetSave(
        args.outputPath,
        args.blockSize,
        args.pageSize,
        args.compressionCodec,
        args.disableDictionaryEncoding,
        Some(flatSchema))
  }
}
