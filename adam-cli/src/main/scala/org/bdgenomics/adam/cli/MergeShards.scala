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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.FileMerger
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

class MergeShardsArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The shard directory to merge", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "The location to write the merged file", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-header_path", usage = "Optional path to a header")
  var headerPath: String = null
  @Args4jOption(required = false,
    name = "-buffer_size",
    usage = "Buffer size for merging single file output. If provided, overrides configured buffer size (default of 4MB).")
  var bufferSize: Int = _
  @Args4jOption(required = false,
    name = "-write_empty_GZIP_at_eof",
    usage = "If provided, writes an empty GZIP block at EOF")
  var gzipAtEof: Boolean = false
  @Args4jOption(required = false,
    name = "-write_cram_eof",
    usage = "If provided, writes the CRAM EOF signifier")
  var cramEof: Boolean = false
}

object MergeShards extends BDGCommandCompanion {
  val commandName = "mergeShards"
  val commandDescription = "Merges the shards of a file"

  def apply(cmdLine: Array[String]): MergeShards = {
    val args = Args4j[MergeShardsArgs](cmdLine)
    new MergeShards(args)
  }
}

/**
 * A command to merge sharded files.
 *
 * This needs to be a Spark command in order to pull in the Hadoop Config via Spark.
 * Also, this allows us to benefit from Spark's YARN/Mesos submission bits, etc.
 */
class MergeShards(val args: MergeShardsArgs) extends BDGSparkCommand[MergeShardsArgs] {
  val companion = MergeShards

  def run(sc: SparkContext) = {
    // write file to disk
    val conf = sc.hadoopConfiguration

    // get file system
    val optHeadPath = Option(args.headerPath).map(p => new Path(p))
    val tailPath = new Path(args.inputPath)
    val outputPath = new Path(args.outputPath)
    val fsIn = tailPath.getFileSystem(conf)
    val fsOut = outputPath.getFileSystem(conf)

    // merge the files
    FileMerger.mergeFilesAcrossFilesystems(conf,
      fsIn, fsOut,
      outputPath, tailPath, optHeadPath,
      writeEmptyGzipBlock = args.gzipAtEof,
      writeCramEOF = args.cramEof,
      optBufferSize = Option(args.bufferSize).filter(_ > 0))
  }
}
