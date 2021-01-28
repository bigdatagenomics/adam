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
package org.bdgenomics.adam.models

import htsjdk.samtools.SAMFileHeader
import org.bdgenomics.adam.ds.ADAMContext
import org.bdgenomics.adam.ds.read.AlignmentDataset
import scala.collection.JavaConversions._

/**
 * @deprecated no longer necessary, SAMFileHeader implements Serializable
 */
private[adam] object SAMFileHeaderWritable {

  /**
   * Creates a serializable representation of a SAM file header.
   *
   * @param header SAMFileHeader to create serializable representation of.
   * @return A serializable representation of the header that can be transformed
   *   back into the htsjdk representation on the executor.
   */
  def apply(header: SAMFileHeader): SAMFileHeaderWritable = {
    new SAMFileHeaderWritable(header)
  }
}

/**
 * Wrapper for the SAM file header to get around serialization issues.
 *
 * The SAM file header is not serialized and is instead recreated on demaind.
 *
 * @param hdr A SAM file header to extract metadata from.
 */
private[adam] class SAMFileHeaderWritable private (hdr: SAMFileHeader) extends Serializable {

  // extract fields that are needed in order to recreate the SAMFileHeader
  private val text = {
    val txt: String = hdr.getTextHeader
    Option(txt)
  }
  private val sd = SequenceDictionary(hdr.getSequenceDictionary)
  private val pgl = {
    val pgs = hdr.getProgramRecords
    pgs.map(ADAMContext.convertSAMProgramRecord)
  }
  private val comments = {
    val cmts = hdr.getComments
    cmts.flatMap(Option(_)) // don't trust samtools to return non-nulls
  }
  private val rgs = ReadGroupDictionary.fromSAMHeader(hdr)

  /**
   * Recreate header when requested to get around header not being serializable.
   */
  @transient lazy val header = {
    val h = new SAMFileHeader()

    // add back optional fields
    text.foreach(h.setTextHeader)
    h.setSequenceDictionary(sd.toSAMSequenceDictionary)
    pgl.foreach(p => h.addProgramRecord(AlignmentDataset.processingStepToSam(p)))
    comments.foreach(h.addComment)
    rgs.readGroups.foreach(rg => h.addReadGroup(rg.toSAMReadGroupRecord))

    h
  }
}
