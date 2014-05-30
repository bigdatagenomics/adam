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

import java.io.{ ByteArrayOutputStream, FileOutputStream, OutputStream }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.bdgenomics.adam.parquet_reimpl.index._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.projections.ADAMFlatGenotypeField._
import org.bdgenomics.formats.avro.ADAMFlatGenotype
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object IndexFlatGenotype extends ADAMCommandCompanion {
  val commandName: String = "index_flat_genotype"
  val commandDescription: String = "Indexes Parquet files containing ADAMFlatGenotype records"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new IndexFlatGenotype(Args4j[IndexFlatGenotypeArgs](cmdLine))
  }
}

class IndexFlatGenotypeArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INDEX", usage = "The index file to generate", index = 0)
  var indexFile: String = null
  @Argument(required = true, metaVar = "PARQUET_LIST", usage = "Comma-separated list of parquet file paths", index = 1)
  var listOfParquetFiles: String = null

  @Args4jOption(required = false, name = "-window", usage = "Window size for combining variants")
  var wSize = 100000
}

class IndexFlatGenotype(@transient val args: IndexFlatGenotypeArgs) extends ADAMSparkCommand[IndexFlatGenotypeArgs]
    with Serializable {
  val companion = IndexFlatGenotype

  def run(sc: SparkContext, job: Job) = {

    val rangeSize = args.wSize

    val writer: OutputStream = new FileOutputStream(args.indexFile)
    sc.parallelize(args.listOfParquetFiles.split(",")).map {
      case parquetFilePath: String =>
        implicit val folder = new ADAMFlatGenotypeReferenceFolder(rangeSize)

        val schema = Projection(referenceName, position, sampleId)
        val bytes = new ByteArrayOutputStream()
        val indexWriter: IDRangeIndexWriter = new IDRangeIndexWriter(bytes)
        folder.count = 0

        val generator: IDRangeIndexGenerator[ADAMFlatGenotype] =
          new IDRangeIndexGenerator[ADAMFlatGenotype](
            (v: ADAMFlatGenotype) => v.getSampleId.toString,
            Some(schema))
        generator.addParquetFile(parquetFilePath).foreach(indexWriter.write)
        indexWriter.flush()
        bytes.toByteArray
    }.collect().foreach {
      case bytes: Array[Byte] =>
        writer.write(bytes)
        writer.flush()
    }

    writer.close()
  }
}

