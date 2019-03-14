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
package org.bdgenomics.adam.util

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.rdd.RDD

/**
 * Writes an RDD to disk as text and optionally merges.
 */
object TextRddWriter {

  /**
   * Writes an RDD to disk as text and optionally merges.
   *
   * @param rdd RDD to save.
   * @param outputPath Output path to save text files to.
   * @param asSingleFile If true, combines all partition shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param optHeaderPath If provided, the header file to include.
   */
  def writeTextRdd[T](rdd: RDD[T],
                      outputPath: String,
                      asSingleFile: Boolean,
                      disableFastConcat: Boolean,
                      optHeaderPath: Option[String] = None) {
    if (asSingleFile) {

      // write RDD to disk
      val tailPath = "%s_tail".format(outputPath)
      rdd.saveAsTextFile(tailPath)

      // get the filesystem impl
      val fs = FileSystem.get(rdd.context.hadoopConfiguration)

      // and then merge
      FileMerger.mergeFiles(rdd.context,
        fs,
        new Path(outputPath),
        new Path(tailPath),
        disableFastConcat = disableFastConcat,
        optHeaderPath = optHeaderPath.map(p => new Path(p)))
    } else {
      assert(optHeaderPath.isEmpty)
      rdd.saveAsTextFile(outputPath)
    }
  }
}
