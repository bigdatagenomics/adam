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
package org.bdgenomics.adam.rdd.read

import htsjdk.samtools.{ SAMFileHeader, SAMFileWriter }
import java.io.OutputStream
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SAMFileHeaderWritable
}
import org.bdgenomics.adam.rdd.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.formats.avro.AlignmentRecord

trait AnySAMInFormatterCompanion[T <: AnySAMInFormatter[T]] extends InFormatterCompanion[AlignmentRecord, AlignmentRecordRDD, T] {
  protected def makeFormatter(header: SAMFileHeaderWritable,
                              recordGroups: RecordGroupDictionary,
                              converter: AlignmentRecordConverter): T

  def apply(gRdd: AlignmentRecordRDD): T = {

    // make a converter
    val arc = new AlignmentRecordConverter

    // build a header and set the sort order
    val header = arc.createSAMHeader(gRdd.sequences, gRdd.recordGroups)
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate)

    // construct the in formatter
    makeFormatter(new SAMFileHeaderWritable(header), gRdd.recordGroups, arc)
  }
}

trait AnySAMInFormatter[T <: AnySAMInFormatter[T]] extends InFormatter[AlignmentRecord, AlignmentRecordRDD, T] {

  val header: SAMFileHeaderWritable
  val recordGroups: RecordGroupDictionary
  val converter: AlignmentRecordConverter

  protected def makeWriter(os: OutputStream): SAMFileWriter

  /**
   * Writes alignment records to an output stream in SAM format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[AlignmentRecord]) {

    // create a sam file writer connected to the output stream
    val writer = makeWriter(os)

    // write the records
    iter.foreach(r => {
      val samRecord = converter.convert(r, header, recordGroups)
      writer.addAlignment(samRecord)
    })

    // close the writer, else stream may be defective
    writer.close()
  }
}
