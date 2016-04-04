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

package org.bdgenomics.adam.io

import htsjdk.samtools.BAMRecordCodec
import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.SAMRecord
import htsjdk.samtools.util.BlockCompressedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.util.WrapSeekable
import org.seqdoop.hadoop_bam.SAMRecordWritable
import org.seqdoop.hadoop_bam.BAMRecordReader
import org.seqdoop.hadoop_bam.FileVirtualSplit
import org.seqdoop.hadoop_bam.BAMInputFormat
import scala.annotation.tailrec

object BAMFilteredRecordReader {
  private var optViewRegion: Option[ReferenceRegion] = None

  def setRegion(viewRegion: ReferenceRegion) {
    optViewRegion = Some(viewRegion)
  }
}

/**
 * Scala implementation of BAMRecordReader, but with
 * nextKeyValue() that filters by ReferenceRegion
 */
class BAMFilteredRecordReader extends BAMRecordReader {

  val key: LongWritable = new LongWritable()
  val record: SAMRecordWritable = new SAMRecordWritable

  var stringency: ValidationStringency = _

  var bci: BlockCompressedInputStream = _
  var codec: BAMRecordCodec = _
  var fileStart: Long = _
  var virtualEnd: Long = _
  var isInitialized: Boolean = false

  override def initialize(spl: InputSplit, ctx: TaskAttemptContext) {
    // Check to ensure this method is only be called once (see Hadoop API)
    if (isInitialized) {
      close()
    }
    isInitialized = true

    val conf: Configuration = ctx.getConfiguration()

    val split: FileVirtualSplit = spl.asInstanceOf[FileVirtualSplit]
    val file: Path = split.getPath()
    val fs: FileSystem = file.getFileSystem(conf)

    this.stringency = SAMHeaderReader.getValidationStringency(conf)

    val in: FSDataInputStream = fs.open(file)

    // Sets codec to translate between in-memory and disk representation of record
    codec = new BAMRecordCodec(SAMHeaderReader.readSAMHeaderFrom(in, conf))

    in.seek(0)

    bci = new BlockCompressedInputStream(
      new WrapSeekable[FSDataInputStream](
        in, fs.getFileStatus(file).getLen(), file))

    // Gets BGZF virtual offset for the split
    val virtualStart = split.getStartVirtualOffset()

    fileStart = virtualStart >>> 16
    virtualEnd = split.getEndVirtualOffset()

    // Starts looking from the BGZF virtual offset
    bci.seek(virtualStart)
    // Reads records from this input stream
    codec.setInputStream(bci)
  }

  override def close() = {
    bci.close()
  }

  override def getCurrentKey(): LongWritable = {
    key
  }

  override def getCurrentValue(): SAMRecordWritable = {
    record
  }

  /**
   * This method gets the nextKeyValue for our RecordReader, but filters by only
   * returning records within a specified ReferenceRegion.
   * This function is tail recursive to avoid stack overflow when predicate data
   * can be sparse.
   */
  @tailrec final override def nextKeyValue(): Boolean = {
    if (bci.getFilePointer() >= virtualEnd) {
      false
    } else {
      val r: SAMRecord = codec.decode()

      // Since we're reading from a BAMRecordCodec directly we have to set the
      // validation stringency ourselves.
      if (this.stringency != null) {
        r.setValidationStringency(this.stringency)
      }

      // This if/else block pushes the predicate down onto a BGZIP block that 
      // the index has said contains data in our specified region.
      if (r == null) {
        false
      } else {
        val start = r.getStart
        val end = r.getEnd
        val refReg = BAMFilteredRecordReader.optViewRegion.get
        val regStart = refReg.start
        val regEnd = refReg.end

        if ((r.getContig() == refReg.referenceName) &&
          (((start >= regStart) && (end <= regEnd))
            || ((start <= regStart) && (end >= regStart) && (end <= regEnd))
            || ((end >= regEnd) && (start >= regStart) && (start <= regEnd)))) {
          key.set(BAMRecordReader.getKey(r))
          record.set(r)
          true
        } else {
          nextKeyValue()
        }
      }
    }
  }

}
