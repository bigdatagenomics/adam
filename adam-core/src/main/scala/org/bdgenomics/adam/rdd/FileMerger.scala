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
package org.bdgenomics.adam.rdd

import htsjdk.samtools.util.BlockCompressedStreamConstants
import java.io.{ InputStream, OutputStream }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.bdgenomics.utils.misc.Logging
import scala.annotation.tailrec

/**
 * Helper object to merge sharded files together.
 */
private[rdd] object FileMerger extends Logging {

  /**
   * Merges together sharded files, while preserving partition ordering.
   *
   * @param fs The file system implementation to use.
   * @param outputPath The location to write the merged file at.
   * @param tailPath The location where the sharded files have been written.
   * @param optHeaderPath Optionally, the location where a header file has
   *   been written.
   * @param writeEmptyGzipBlock If true, we write an empty GZIP block at the
   *   end of the merged file.
   * @param bufferSize The size in bytes of the buffer used for copying.
   */
  def mergeFiles(fs: FileSystem,
                 outputPath: Path,
                 tailPath: Path,
                 optHeaderPath: Option[Path] = None,
                 writeEmptyGzipBlock: Boolean = false,
                 bufferSize: Int = 1024) {

    // get a list of all of the files in the tail file
    val tailFiles = fs.globStatus(new Path("%s/part-*".format(tailPath)))
      .toSeq
      .map(_.getPath)
      .sortBy(_.getName)
      .toArray

    // doing this correctly is surprisingly hard
    // specifically, copy merge does not care about ordering, which is
    // fine if your files are unordered, but if the blocks in the file
    // _are_ ordered, then hahahahahahahahahaha. GOOD. TIMES.
    //
    // fortunately, the blocks in our file are ordered
    // the performance of this section is hilarious
    // 
    // specifically, the performance is hilariously bad
    //
    // but! it is correct.

    // open our output file
    val os = fs.create(outputPath)

    // here is a byte array for copying
    val ba = new Array[Byte](bufferSize)

    @tailrec def copy(is: InputStream,
                      los: OutputStream) {

      // make a read
      val bytesRead = is.read(ba)

      // did our read succeed? if so, write to output stream
      // and continue
      if (bytesRead >= 0) {
        los.write(ba, 0, bytesRead)

        copy(is, los)
      }
    }

    // optionally copy the header
    optHeaderPath.foreach(p => {
      log.info("Copying header file (%s)".format(p))

      // open our input file
      val is = fs.open(p)

      // until we are out of bytes, copy
      copy(is, os)

      // close our input stream
      is.close()
    })

    // loop over allllll the files and copy them
    val numFiles = tailFiles.length
    var filesCopied = 1
    tailFiles.toSeq.foreach(p => {

      // print a bit of progress logging
      log.info("Copying file %s, file %d of %d.".format(
        p.toString,
        filesCopied,
        numFiles))

      // open our input file
      val is = fs.open(p)

      // until we are out of bytes, copy
      copy(is, os)

      // close our input stream
      is.close()

      // increment file copy count
      filesCopied += 1
    })

    // finish the file off
    if (writeEmptyGzipBlock) {
      os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
    }

    // flush and close the output stream
    os.flush()
    os.close()

    // delete temp files
    optHeaderPath.foreach(headPath => fs.delete(headPath, true))
    fs.delete(tailPath, true)
  }
}
