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

package htsjdk.samtools

import htsjdk.samtools.util.BlockCompressedFilePointerUtil
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.seqdoop.hadoop_bam.BAMInputFormat
import org.seqdoop.hadoop_bam.FileVirtualSplit
import org.seqdoop.hadoop_bam.SAMRecordWritable
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.io.BAMFilteredRecordReader
import org.apache.hadoop.io.LongWritable
import java.io.File
import scala.collection.mutable

object IndexedBamInputFormat {

  var optFilePath: Option[Path] = None
  var optIndexFilePath: Option[Path] = None
  var optViewRegion: Option[ReferenceRegion] = None
  var optDict: Option[SAMSequenceDictionary] = None

  def apply(filePath: Path, indexFilePath: Path, viewRegion: ReferenceRegion, dict: SAMSequenceDictionary) {
    optFilePath = Some(filePath)
    optIndexFilePath = Some(indexFilePath)
    optViewRegion = Some(viewRegion)
    optDict = Some(dict)
  }

}

class IndexedBamInputFormat extends BAMInputFormat {

  override def createRecordReader(split: InputSplit, ctx: TaskAttemptContext): RecordReader[LongWritable, SAMRecordWritable] = {
    val rr: RecordReader[LongWritable, SAMRecordWritable] = new BAMFilteredRecordReader()
    assert(IndexedBamInputFormat.optViewRegion.isDefined)
    BAMFilteredRecordReader(IndexedBamInputFormat.optViewRegion.get)
    rr.initialize(split, ctx)
    rr
  }

  override def getSplits(job: JobContext): java.util.List[InputSplit] = {
    assert(IndexedBamInputFormat.optIndexFilePath.isDefined &&
      IndexedBamInputFormat.optFilePath.isDefined &&
      IndexedBamInputFormat.optViewRegion.isDefined &&
      IndexedBamInputFormat.optDict.isDefined)

    val indexFilePath = IndexedBamInputFormat.optIndexFilePath.get

    val idxFile: File = new File(indexFilePath.toString)
    if (!idxFile.exists()) {
      super.getSplits(job)
    } else {
      val filePath = IndexedBamInputFormat.optFilePath.get
      val viewRegion = IndexedBamInputFormat.optViewRegion.get
      val refName = viewRegion.referenceName
      val dict = IndexedBamInputFormat.optDict.get
      val start = viewRegion.start.toInt
      val end = viewRegion.end.toInt
      val dbbfi: DiskBasedBAMFileIndex = new DiskBasedBAMFileIndex(idxFile, dict)
      val referenceIndex: Int = dict.getSequenceIndex(refName)
      var regions: List[Chunk] = dbbfi.getSpanOverlapping(referenceIndex, start, end).getChunks

      var splits = new mutable.ListBuffer[FileVirtualSplit]()
      for (chunk <- regions) {
        val start: Long = chunk.getChunkStart()
        val end: Long = chunk.getChunkEnd()
        val locs = Array[String]()
        val newSplit = new FileVirtualSplit(filePath, start, end, locs)
        splits += newSplit
      }
      splits.toList
    }
  }

}
