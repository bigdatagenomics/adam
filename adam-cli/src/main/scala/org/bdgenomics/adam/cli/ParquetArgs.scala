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

import org.kohsuke.args4j.Option
import parquet.hadoop.metadata.CompressionCodecName

trait ParquetArgs extends Args4jBase {
  @Option(required = false, name = "-parquet_block_size", usage = "Parquet block size (default = 128mb)")
  var blockSize = 128 * 1024 * 1024
  @Option(required = false, name = "-parquet_page_size", usage = "Parquet page size (default = 1mb)")
  var pageSize = 1 * 1024 * 1024
  @Option(required = false, name = "-parquet_compression_codec", usage = "Parquet compression codec")
  var compressionCodec = CompressionCodecName.GZIP
  @Option(name = "-parquet_disable_dictionary", usage = "Disable dictionary encoding")
  var disableDictionary = false
  @Option(required = false, name = "-parquet_logging_level", usage = "Parquet logging level (default = severe)")
  var logLevel = "SEVERE"
}

