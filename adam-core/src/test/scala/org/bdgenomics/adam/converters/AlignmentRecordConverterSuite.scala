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
package org.bdgenomics.adam.converters

import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SAMFileHeaderWritable,
  SequenceDictionary,
  SequenceRecord
}
import org.scalatest.FunSuite

class AlignmentRecordConverterSuite extends FunSuite {

  // allocate converters
  val adamRecordConverter = new AlignmentRecordConverter

  def make_read(start: Long, cigar: String, mdtag: String, length: Int, id: Int = 0): AlignmentRecord = {
    val sequence: String = "A" * length
    AlignmentRecord.newBuilder()
      .setReadName("read" + id.toString)
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setQual(sequence) // no typo, we just don't care
      .setMismatchingPositions(mdtag)
      .setOldPosition(12)
      .setOldCigar("2^AAA3")
      .build()
  }

  test("testing the fields in a converted ADAM Read") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)

    // add reference details
    adamRead.setRecordGroupName("testname")
    adamRead.setContig(Contig.newBuilder()
      .setContigName("referencetest")
      .build())
    adamRead.setMateContig(Contig.newBuilder()
      .setContigName("matereferencetest")
      .setContigLength(6L)
      .setReferenceURL("test://chrom1")
      .build())
    adamRead.setMateAlignmentStart(6L)

    // make sequence dictionary
    val seqRecForDict = SequenceRecord("referencetest", 5, "test://chrom1")
    val dict = SequenceDictionary(seqRecForDict)
    val readGroups = new RecordGroupDictionary(Seq())

    // convert read
    val toSAM = adamRecordConverter.convert(adamRead,
      SAMFileHeaderWritable(adamRecordConverter.createSAMHeader(dict,
        readGroups)))

    // validate conversion
    val sequence = "A" * 4
    assert(toSAM.getReadName === ("read" + 0.toString))
    assert(toSAM.getAlignmentStart === 4)
    assert(toSAM.getReadUnmappedFlag === false)
    assert(toSAM.getCigarString === "2M3D2M")
    assert(toSAM.getReadString === sequence)
    assert(toSAM.getReadNegativeStrandFlag === false)
    assert(toSAM.getMappingQuality === 60)
    assert(toSAM.getBaseQualityString === sequence)
    assert(toSAM.getAttribute("MD") === "2^AAA2")
    assert(toSAM.getIntegerAttribute("OP") === 13)
    assert(toSAM.getStringAttribute("OC") === "2^AAA3")
  }

  test("convert a read to fastq") {
    val adamRead = AlignmentRecord.newBuilder()
      .setSequence("ACACCAACATG")
      .setQual(".+**.+;:**.")
      .setReadName("thebestread")
      .build()

    val fastq = adamRecordConverter.convertToFastq(adamRead)
      .toString
      .split('\n')

    assert(fastq(0) === "@thebestread")
    assert(fastq(1) === "ACACCAACATG")
    assert(fastq(2) === "+")
    assert(fastq(3) === ".+**.+;:**.")
  }
}

