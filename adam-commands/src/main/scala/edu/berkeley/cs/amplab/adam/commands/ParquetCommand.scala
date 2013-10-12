/*
 * Copyright (c) 2013. Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.commands

import edu.berkeley.cs.amplab.adam.util.Args4jBase
import org.kohsuke.args4j.Option
import parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.Schema
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}

trait ParquetArgs extends Args4jBase {
  @Option(required = false, name = "-parquet_block_size", usage = "Parquet block size (default = 512mb)")
  var blockSize = 512 * 1024 * 1024
  @Option(required = false, name = "-parquet_page_size", usage = "Parquet page size (default = 1mb)")
  var pageSize = 1 * 1024 * 1024
  @Option(name = "-parquet_compress", usage = "Parquet compress (default = true)")
  var compress = true
  @Option(required = false, name = "-parquet_compression_codec", usage = "Parquet compression codec (default = gzip)")
  var compressionCodec = CompressionCodecName.GZIP.toString
  @Option(name = "-parquet_disable_dictionary", usage = "Disable dictionary encoding. (default = false)")
  var disableDictionary = false
}

trait ParquetCommand extends AdamCommand {

  def setupParquetOutputFormat(args: ParquetArgs, job: Job, schema: Schema) {
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setCompression(job, CompressionCodecName.fromConf(args.compressionCodec))
    ParquetOutputFormat.setEnableDictionary(job, !args.disableDictionary)
    ParquetOutputFormat.setBlockSize(job, args.blockSize)
    ParquetOutputFormat.setPageSize(job, args.pageSize)
    AvroParquetOutputFormat.setSchema(job, schema)
  }


}
