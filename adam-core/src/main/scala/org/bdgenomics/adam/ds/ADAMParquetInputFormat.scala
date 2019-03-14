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
package org.bdgenomics.adam.ds

import java.io.IOException

import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext }
import org.apache.parquet.hadoop.ParquetInputFormat
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Ensures that our data is read and written in the same order.
 *
 * @note listStatus does not guarantee order, so the simplest solution here
 *       was to override the getSplits and sort after we get all the splits,
 *       while disallowing splitting within a file. This guarantees that our
 *       data is read as it was written. Sorting overhead will be low because
 *       we are only sorting a number of elements equal to the number of
 *       partitions written.
 */
private[ds] class ADAMParquetInputFormat[T] extends ParquetInputFormat[T] {

  /**
   * Gets the splits of all the files for the JobContext object.
   *
   * @param job The JobContext object.
   * @return A java list containing the InputSplits for the job.
   * @throws IOException When a block is not found.
   */
  override def getSplits(job: JobContext): java.util.List[InputSplit] = {

    val splits = new ListBuffer[InputSplit]()
    // iterate over all files (in no particular order)
    for (file <- listStatus(job)) {
      val path = file.getPath
      val length = file.getLen

      if (length != 0) {
        val fs = path.getFileSystem(job.getConfiguration)
        val blkLocations = fs.getFileBlockLocations(file, 0, length)
        // getFileBlockLocations returns null if the block is not found
        if (blkLocations == null) {
          throw new IOException("File Block %s not found.".format(file.getPath))
        }
        // blkLocations contains the locations of all copies of the block
        // we use blkLocations(0) because it is the "original" data, as in not
        // an HDFS replication
        splits.add(new FileSplit(path, 0, length, blkLocations(0).getHosts))
      } else {
        splits.add(new FileSplit(path, 0, length, Array.ofDim[String](0)))
      }
    }
    // sort by path first, then split offset
    splits.sortBy(_.toString).toList
  }
}
