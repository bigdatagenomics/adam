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

import java.io.{ InputStream, OutputStream }
import grizzled.slf4j.Logging
import htsjdk.samtools.cram.build.CramIO
import htsjdk.samtools.cram.common.CramVersions
import htsjdk.samtools.util.BlockCompressedStreamConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import scala.annotation.tailrec

/**
 * Helper object to merge sharded files together.
 *
 * @see ParallelFileMerger
 */
private[adam] object FileMerger extends Logging {

  /**
   * The config entry for the buffer size in bytes.
   */
  val BUFFER_SIZE_CONF = "org.bdgenomics.adam.rdd.FileMerger.bufferSize"

  private def isHdfs(fs: FileSystem): Boolean = {
    try {
      fs.getScheme == "hdfs"
    } catch {
      case _: Throwable => false
    }
  }

  /**
   * Merges together sharded files, while preserving partition ordering.
   *
   * Automatically checks to see if the filesystem is HDFS, and if so, uses the
   * parallel file merging implementation to concatenate files.
   *
   * @param sc Spark context.
   * @param fs The file system implementation to use.
   * @param outputPath The location to write the merged file at.
   * @param tailPath The location where the sharded files have been written.
   * @param optHeaderPath Optionally, the location where a header file has
   *   been written.
   * @param writeEmptyGzipBlock If true, we write an empty GZIP block at the
   *   end of the merged file.
   * @param writeCramEOF If true, we write CRAM's EOF signifier.
   * @param optBufferSize The size in bytes of the buffer used for copying. If
   *   not set, we check the config for this value. If that is not set, we
   *   default to 4MB.
   * @param disableFastConcat If true, disables the parallel file merger. By
   *   default, the fast file merger is invoked when running on HDFS. However,
   *   the fast file merger can fail if the underlying file system is encrypted,
   *   or any number of undocumented invariants are not met. In that case, we
   *   provide this switch to disable fast merging.
   * @see mergeFilesAcrossFilesystems
   */
  def mergeFiles(sc: SparkContext,
                 fs: FileSystem,
                 outputPath: Path,
                 tailPath: Path,
                 optHeaderPath: Option[Path] = None,
                 writeEmptyGzipBlock: Boolean = false,
                 writeCramEOF: Boolean = false,
                 optBufferSize: Option[Int] = None,
                 disableFastConcat: Boolean = false) {

    // if our file system is an hdfs mount, we can use the parallel merger
    if (!disableFastConcat && isHdfs(fs)) {
      ParallelFileMerger.mergeFiles(sc,
        outputPath,
        tailPath,
        optHeaderPath = optHeaderPath,
        writeEmptyGzipBlock = writeEmptyGzipBlock,
        writeCramEOF = writeCramEOF,
        optBufferSize = optBufferSize)
    } else {
      mergeFilesAcrossFilesystems(sc.hadoopConfiguration,
        fs, fs,
        outputPath,
        tailPath,
        optHeaderPath = optHeaderPath,
        writeEmptyGzipBlock = writeEmptyGzipBlock,
        writeCramEOF = writeCramEOF,
        optBufferSize = optBufferSize)
    }
  }

  /**
   * Merges together sharded files, while preserving partition ordering.
   *
   * Can read files from a different filesystem then they are written to.
   *
   * @param conf Hadoop configuration.
   * @param fsIn The file system implementation to use for the tail/head paths.
   * @param fsOut The file system implementation to use for the output path.
   * @param outputPath The location to write the merged file at.
   * @param tailPath The location where the sharded files have been written.
   * @param optHeaderPath Optionally, the location where a header file has
   *   been written.
   * @param writeEmptyGzipBlock If true, we write an empty GZIP block at the
   *   end of the merged file.
   * @param writeCramEOF If true, we write CRAM's EOF signifier.
   * @param optBufferSize The size in bytes of the buffer used for copying. If
   *   not set, we check the config for this value. If that is not set, we
   *   default to 4MB.
   */
  def mergeFilesAcrossFilesystems(conf: Configuration,
                                  fsIn: FileSystem,
                                  fsOut: FileSystem,
                                  outputPath: Path,
                                  tailPath: Path,
                                  optHeaderPath: Option[Path] = None,
                                  writeEmptyGzipBlock: Boolean = false,
                                  writeCramEOF: Boolean = false,
                                  optBufferSize: Option[Int] = None) {

    // check for buffer size in option, if not in option, check hadoop conf,
    // if not in hadoop conf, fall back on 4MB
    val bufferSize = optBufferSize.getOrElse(conf.getInt(BUFFER_SIZE_CONF,
      4 * 1024 * 1024))

    require(bufferSize > 0,
      "Cannot have buffer size < 1. %d was provided.".format(bufferSize))
    require(!(writeEmptyGzipBlock && writeCramEOF),
      "writeEmptyGzipBlock and writeCramEOF are mutually exclusive.")

    // get a list of all of the files in the tail file
    val tailFiles = fsIn.globStatus(new Path("%s/part-*".format(tailPath)))
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
    val os = fsOut.create(outputPath)

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
      info("Copying header file (%s)".format(p))

      // open our input file
      val is = fsIn.open(p)

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
      info("Copying file %s, file %d of %d.".format(
        p.toString,
        filesCopied,
        numFiles))

      // open our input file
      val is = fsIn.open(p)

      // until we are out of bytes, copy
      copy(is, os)

      // close our input stream
      is.close()

      // increment file copy count
      filesCopied += 1
    })

    // finish the file off
    if (writeEmptyGzipBlock) {
      os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    } else if (writeCramEOF) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, os)
    }

    // flush and close the output stream
    os.flush()
    os.close()

    // delete temp files
    optHeaderPath.foreach(headPath => fsIn.delete(headPath, true))
    fsIn.delete(tailPath, true)
  }
}
