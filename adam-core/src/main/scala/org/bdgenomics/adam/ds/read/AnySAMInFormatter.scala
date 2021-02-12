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
package org.bdgenomics.adam.ds.read

import htsjdk.samtools.{ SAMFileHeader, SAMFileWriter }
import java.io.OutputStream
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.models.ReadGroupDictionary
import org.bdgenomics.adam.ds.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.adam.sql.{ Alignment => AlignmentProduct }
import org.bdgenomics.formats.avro.Alignment

/**
 * Companion object that builds an InFormatter that writes data where the metadata
 * is contained in a SAMFileHeaderWritable.
 *
 * @tparam T The type of the underlying InFormatter.
 */
trait AnySAMInFormatterCompanion[T <: AnySAMInFormatter[T]] extends InFormatterCompanion[Alignment, AlignmentProduct, AlignmentDataset, T] {
  protected def makeFormatter(header: SAMFileHeader,
                              readGroups: ReadGroupDictionary,
                              converter: AlignmentConverter): T

  /**
   * Makes an AnySAMInFormatter from a GenomicDataset of Alignments.
   *
   * @param gDataset AlignmentDataset with reference build and read group info.
   * @return Returns an InFormatter that extends AnySAMInFormatter.
   */
  def apply(gDataset: AlignmentDataset): T = {

    // make a converter
    val arc = new AlignmentConverter

    // build a header and set the sort order
    val header = arc.createSAMHeader(gDataset.references, gDataset.readGroups)
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate)

    // construct the in formatter
    makeFormatter(header, gDataset.readGroups, arc)
  }
}

/**
 * A trait that writes reads using an Htsjdk SAMFileWriter.
 *
 * @tparam T The recursive type of the class that implements this trait.
 */
trait AnySAMInFormatter[T <: AnySAMInFormatter[T]] extends InFormatter[Alignment, AlignmentProduct, AlignmentDataset, T] {

  /**
   * A serializable form of the SAM File Header.
   */
  val header: SAMFileHeader

  /**
   * A dictionary describing the read groups these reads are from.
   */
  val readGroups: ReadGroupDictionary

  /**
   * A converter from Alignment to SAMRecord.
   */
  val converter: AlignmentConverter

  protected def makeWriter(os: OutputStream): SAMFileWriter

  /**
   * Writes alignments to an output stream in SAM format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[Alignment]) {

    // create a sam file writer connected to the output stream
    val writer = makeWriter(os)

    // write the records
    iter.foreach(r => {
      val samRecord = converter.convert(r, header, readGroups)
      writer.addAlignment(samRecord)
    })

    // close the writer, else stream may be defective
    writer.close()
  }
}
