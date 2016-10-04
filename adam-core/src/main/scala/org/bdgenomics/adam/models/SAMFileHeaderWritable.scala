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

import htsjdk.samtools.{ SAMFileHeader, SAMProgramRecord }
import scala.collection.JavaConversions._

object SAMFileHeaderWritable {
  def apply(header: SAMFileHeader): SAMFileHeaderWritable = {
    new SAMFileHeaderWritable(header)
  }
}

class SAMFileHeaderWritable(hdr: SAMFileHeader) extends Serializable {
  // extract fields that are needed in order to recreate the SAMFileHeader
  protected val text = {
    val txt: String = hdr.getTextHeader
    Option(txt)
  }
  protected val sd = SequenceDictionary(hdr.getSequenceDictionary)
  protected val pgl = {
    val pgs = hdr.getProgramRecords
    pgs.map(ProgramRecord(_))
  }
  protected val comments = {
    val cmts = hdr.getComments
    cmts.flatMap(Option(_)) // don't trust samtools to return non-nulls
  }
  protected val rgs = RecordGroupDictionary.fromSAMHeader(hdr)

  // recreate header when requested to get around header not being serializable
  @transient lazy val header = {
    val h = new SAMFileHeader()

    // add back optional fields
    text.foreach(h.setTextHeader)
    h.setSequenceDictionary(sd.toSAMSequenceDictionary)
    pgl.foreach(p => h.addProgramRecord(p.toSAMProgramRecord))
    comments.foreach(h.addComment)
    rgs.recordGroups.foreach(rg => h.addReadGroup(rg.toSAMReadGroupRecord))

    h
  }
}
