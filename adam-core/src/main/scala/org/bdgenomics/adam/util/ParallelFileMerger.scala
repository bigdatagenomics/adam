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

import grizzled.slf4j.Logging
import htsjdk.samtools.cram.build.CramIO
import htsjdk.samtools.cram.common.CramVersions
import htsjdk.samtools.util.BlockCompressedStreamConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.annotation.tailrec
import scala.math.min

/**
 * @see FileMerger
 */
private[adam] object ParallelFileMerger extends Logging {

  /**
   * Merges ranges from several files into a single file.
   *
   * @param fileToWrite The location to write the merged file.
   * @param filesToMerge The files to merge. Files are described as a tuple
   *   with the path to load the shard from, and the start and end byte range
   *   to copy.
   * @param broadcastConf A broadcast Hadoop configuration. Must be broadcast
   *   to work around closure cleaning issues.
   * @param writeEmptyGzipBlock If true, we will write an empty GZIP block
   *   at the end of the merged file. This is necessary if writing BAM to not
   *   get file termination warnings.
   * @param writeCramEOF If true, we will write an empty CRAM container at
   *   the end of the merged file. This is necessary if merging CRAM.
   * @param bufferSize The size of a buffer to use for copying data.
   */
  def mergePaths(fileToWrite: String,
                 filesToMerge: Seq[(String, Long, Long)],
                 broadcastConf: Broadcast[Configuration],
                 writeEmptyGzipBlock: Boolean,
                 writeCramEOF: Boolean,
                 bufferSize: Int = 1024) {

    // path is not serializable, so make from string
    val pathToWrite = new Path(fileToWrite)
    val pathsToMerge = filesToMerge.map(t => (new Path(t._1), t._2, t._3))

    val conf = broadcastConf.value
    val fs = pathToWrite.getFileSystem(conf)

    // get an output stream
    val os = fs.create(pathToWrite)

    // allocate the buffer
    val buffer = new Array[Byte](bufferSize)

    // loop and copy the paths to read
    pathsToMerge.foreach(t => {
      val (pathToMerge, start, end) = t

      val is = fs.open(pathToMerge)

      @tailrec def doCopy(pos: Long) {
        if (pos < end) {
          val bytesToCopy = min(bufferSize.toLong, end - pos + 1).toInt

          is.readFully(pos, buffer, 0, bytesToCopy)
          os.write(buffer, 0, bytesToCopy)

          doCopy(pos + bytesToCopy.toLong)
        }
      }

      // copy the file to the output stream
      doCopy(start)

      // close the input stream
      is.close()
    })

    // if requested, finish the file off
    if (writeEmptyGzipBlock) {
      os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    } else if (writeCramEOF) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, os)
    }

    os.flush()
    os.close()
  }

  /**
   * Generates the file/byte ranges to be merged.
   *
   * The contract that the Hadoop DistributedFileSystem.concat method requires
   * (but does not document!) is that all files to be merged must be exactly the
   * file system block size, except for the last block, which must be less than
   * or equal to the block size. This method scans over the blocks that we have
   * and chunks them into a list of separate merges we must do to achieve this
   * contract.
   *
   * @param blockSize The file system block size to use.
   * @param files The files to merge.
   * @return Returns a collection of file ranges to merge into shards. Each
   *   element of this collection is a tuple with the index for the output
   *   shard, and the range of files to merge. The second list contains tuples
   *   with the file path to merge, the start index into the file, and the
   *   end index of the file.
   */
  def generateMerges(
    blockSize: Long,
    files: Seq[(Path, Long)]): Seq[(Int, Seq[(String, Long, Long)])] = {

    assert(blockSize > 0)
    assert(files.nonEmpty)

    @tailrec def addToBlock(
      currentFile: Path,
      currentPosition: Long,
      bytesInFile: Long,
      currentBlockIdx: Int,
      currentBlock: List[(String, Long, Long)],
      currentBlockSize: Long,
      blocks: List[(Int, Seq[(String, Long, Long)])],
      fileIter: Iterator[(Path, Long)]): Seq[(Int, Seq[(String, Long, Long)])] = {

      if (bytesInFile < 0) {
        assert(!fileIter.hasNext)
        if (currentBlock.isEmpty) {
          blocks.toSeq
        } else {
          ((currentBlockIdx, currentBlock.toSeq.reverse) :: blocks).toSeq
        }
      } else {
        val bytesLeftInFile = (bytesInFile - currentPosition)
        val bytesLeftInBlock = (blockSize - currentBlockSize)
        val bytesToTakeFromFile = min(bytesLeftInFile, bytesLeftInBlock)

        val (nextFile, nextPosition, nextBytesInFile,
          nextBlockIdx, nextBlock, nextBlockSize,
          nextBlocks) = if (bytesLeftInFile >= bytesLeftInBlock) {

          // if we are consuming all of the bytes left in the current file to merge,
          // we need to move to the next file
          val (nFile, nPosition, nBytesInFile) = if (bytesToTakeFromFile == bytesLeftInFile) {
            if (fileIter.hasNext) {
              val (headPath, headSize) = fileIter.next
              (headPath, 0L, headSize)
            } else {
              (currentFile, 0L, Long.MinValue)
            }
          } else {
            (currentFile, currentPosition + bytesToTakeFromFile, bytesInFile)
          }

          val finalFilesInBlock = (currentFile.toString,
            currentPosition,
            currentPosition + bytesToTakeFromFile - 1) :: currentBlock

          (nFile, nPosition, nBytesInFile,
            currentBlockIdx + 1, List.empty, 0L,
            (currentBlockIdx, finalFilesInBlock.reverse) :: blocks)
        } else {
          val (nextFile, nextFileLength) = if (fileIter.hasNext) {
            fileIter.next
          } else {
            (currentFile, Long.MinValue)
          }
          val fileSlice = (currentFile.toString, currentPosition, bytesInFile - 1)

          (nextFile, 0L, nextFileLength,
            currentBlockIdx, fileSlice :: currentBlock, currentBlockSize + bytesToTakeFromFile,
            blocks)
        }

        addToBlock(nextFile, nextPosition, nextBytesInFile,
          nextBlockIdx, nextBlock, nextBlockSize,
          nextBlocks,
          fileIter)
      }
    }

    val (firstFile, firstFileSize) = files.head

    addToBlock(firstFile, 0, firstFileSize,
      0, List.empty, 0,
      List.empty,
      files.tail.toIterator)
  }

  /**
   * @param idx The shard index.
   * @param basePath The path of the directory to write the shard in.
   * @return Returns the path to write this shard at.
   */
  def indexToPath(
    idx: Int,
    basePath: String): Path = {
    assert(idx >= 0)

    new Path(basePath).suffix("_part-r-%05d".format(idx))
  }

  /**
   * Computes the combined size of all of the shards to merge.
   *
   * @param fs The file system that the shards reside on.
   * @param files The files to merge.
   * @return Returns a tuple containing the size of the files in bytes and the
   *   paths zipped with their sizes.
   */
  def getFullSize(fs: FileSystem,
                  files: Seq[Path]): (Long, Seq[(Path, Long)]) = {

    val fileSizes = files.map(path => fs.getFileStatus(path).getLen().toLong)

    (fileSizes.sum, files.zip(fileSizes))
  }

  /**
   * Merges together sharded files, while preserving partition ordering.
   *
   * @param sc Spark context.
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
  def mergeFiles(sc: SparkContext,
                 outputPath: Path,
                 tailPath: Path,
                 optHeaderPath: Option[Path] = None,
                 writeEmptyGzipBlock: Boolean = false,
                 writeCramEOF: Boolean = false,
                 optBufferSize: Option[Int] = None) {

    val conf = sc.hadoopConfiguration

    // check for buffer size in option, if not in option, check hadoop conf,
    // if not in hadoop conf, fall back on 4MB
    val bufferSize = optBufferSize.getOrElse(conf.getInt(FileMerger.BUFFER_SIZE_CONF,
      4 * 1024 * 1024))

    require(bufferSize > 0,
      "Cannot have buffer size < 1. %d was provided.".format(bufferSize))
    require(!(writeEmptyGzipBlock && writeCramEOF),
      "writeEmptyGzipBlock and writeCramEOF are mutually exclusive.")

    // get our file system
    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)

    // if provided, prepend the head file to the list of files
    val tailFiles = fs.globStatus(new Path("%s/part-*".format(tailPath)))
      .toSeq
      .map(_.getPath)
      .sortBy(_.getName)

    val allFiles = optHeaderPath.fold(tailFiles)(headerPath => {
      headerPath +: tailFiles
    })

    // get the size of all of our files
    val (totalSize, sizeByFile) = getFullSize(fs, allFiles)

    // get our file system block size
    val blockSize = fs.getDefaultBlockSize(outputPath)

    // generate the merge list
    val merges = generateMerges(blockSize, sizeByFile)

    // make a temp output path
    //
    // ideally, this would be a directory, however, fs.concat has the
    // undocumented contract that the paths being merged must live in
    // the same directory as the path they are being merged to
    val tmpPath = outputPath.suffix("_merging")

    // org.apache.hadoop.fs.Path is not serializable
    val tmpPathString = tmpPath.toString

    // broadcast and repartition the files, and then do the writes
    val rdd = sc.parallelize(merges)
      .partitionBy(ManualRegionPartitioner(merges.size))
    val numBlocksToWrite = merges.size
    val broadcastConf = sc.broadcast(conf)
    val blocksWritten = rdd.foreach(merge => {
      val (idx, blocks) = merge
      val pathToWrite = indexToPath(idx, tmpPathString).toString
      val (gzipAtEnd, cramAtEnd) = if (idx == numBlocksToWrite - 1) {
        (writeEmptyGzipBlock, writeCramEOF)
      } else {
        (false, false)
      }

      mergePaths(pathToWrite,
        blocks,
        broadcastConf,
        gzipAtEnd,
        cramAtEnd,
        bufferSize)
    })

    // UNDOCUMENTED in hadoop fs API:
    // all paths passed to the concat method must be qualified with
    // full scheme and name node URI
    val outputPaths = (0 until numBlocksToWrite).map(idx => {
      val maybeUnqualifiedPath = indexToPath(idx, tmpPathString)
      fs.makeQualified(maybeUnqualifiedPath)
    })
    if (outputPaths.size > 1) {
      fs.concat(outputPaths.head,
        outputPaths.tail.toArray)
    }

    fs.rename(outputPaths.head, outputPath)
  }
}
