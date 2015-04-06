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

import org.scalatest.FunSuite
import org.bdgenomics.adam.models.{ SequenceRecord, Attribute, RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import java.io.File
import htsjdk.samtools._

import scala.collection.JavaConversions._

class SAMRecordConverterSuite extends FunSuite {

  test("testing the fields in an alignmentRecord obtained from a mapped samRecord conversion") {

    val testRecordConverter = new SAMRecordConverter
    val testFileString = getClass.getClassLoader.getResource("reads12.sam").getFile
    val testFile = new File(testFileString)

    //Iterator of SamReads in the file that each have a samRecord for conversion
    val testIterator = SamReaderFactory.makeDefault().open(testFile)
    val testSAMRecord = testIterator.iterator().next()

    //Creating the two SequenceRecords in file for SequenceDictionary
    val testSequenceRecord1 = new SequenceRecord("1", 249250621L)
    val testSequenceRecord2 = new SequenceRecord("2", 243199373L)

    //SequenceDictionary to be used as parameter during conversion
    val testSequenceDict = SequenceDictionary(testSequenceRecord1, testSequenceRecord2)

    //RecordGroupDictionary to be used as a parameter during conversion
    val testRecordGroupDict = new RecordGroupDictionary(Seq())

    //Convert samRecord to alignmentRecord
    val testAlignmentRecord = testRecordConverter.convert(testSAMRecord, testSequenceDict, testRecordGroupDict)

    //Validating Conversion
    assert(testAlignmentRecord.getCigar === testSAMRecord.getCigarString)
    assert(testAlignmentRecord.getDuplicateRead === testSAMRecord.getDuplicateReadFlag)
    assert(testAlignmentRecord.getEnd.toInt === testSAMRecord.getAlignmentEnd)
    assert(testAlignmentRecord.getMapq.toInt === testSAMRecord.getMappingQuality)
    assert(testAlignmentRecord.getStart.toInt === (testSAMRecord.getAlignmentStart - 1))
    assert(testAlignmentRecord.getQual === testSAMRecord.getBaseQualityString)
    assert(!testAlignmentRecord.getFirstOfPair)
    assert(testAlignmentRecord.getFailedVendorQualityChecks === testSAMRecord.getReadFailsVendorQualityCheckFlag)
    assert(!testAlignmentRecord.getPrimaryAlignment === testSAMRecord.getNotPrimaryAlignmentFlag)
    assert(!testAlignmentRecord.getReadMapped === testSAMRecord.getReadUnmappedFlag)
    assert(testAlignmentRecord.getReadName === testSAMRecord.getReadName)
    assert(testAlignmentRecord.getReadNegativeStrand === testSAMRecord.getReadNegativeStrandFlag)
    assert(!testAlignmentRecord.getReadPaired)
    assert(!testAlignmentRecord.getSecondOfPair)
    assert(testAlignmentRecord.getSupplementaryAlignment === testSAMRecord.getSupplementaryAlignmentFlag)

  }

  test("testing the fields in an alignmentRecord obtained from an unmapped samRecord conversion") {

    val newRecordConverter = new SAMRecordConverter
    val newTestFile = new File(getClass.getClassLoader.getResource("unmapped.sam").getFile)
    val newSAMReader = SamReaderFactory.makeDefault().open(newTestFile)

    //Obtain SAMRecord
    val newSAMRecordIter = {
      val samIter = asScalaIterator(newSAMReader.iterator())
      samIter.toIterable.dropWhile(!_.getReadUnmappedFlag())
    }
    val newSAMRecord = newSAMRecordIter.toIterator.next()

    //SequenceRecord to be put into SequenceDictionary
    val newSequenceRecord = new SequenceRecord("1", 249250621L)

    //SequenceDictionary for conversion method
    val newSequenceDictionary = SequenceDictionary(newSequenceRecord)

    //No RecordGroup present in file so I'll just make the RecordGroupDictionary Empty
    val newTestRecordGroupDictionary = new RecordGroupDictionary(Seq())

    //Conversion
    val newAlignmentRecord = newRecordConverter.convert(newSAMRecord, newSequenceDictionary, newTestRecordGroupDictionary)

    //Validating Conversion
    assert(newAlignmentRecord.getCigar === newSAMRecord.getCigarString)
    assert(newAlignmentRecord.getDuplicateRead === newSAMRecord.getDuplicateReadFlag)
    assert(newAlignmentRecord.getEnd === null)
    assert(newAlignmentRecord.getMapq === null)
    assert(newAlignmentRecord.getStart === null)
    assert(newAlignmentRecord.getQual === newSAMRecord.getBaseQualityString)
    assert(!newAlignmentRecord.getFirstOfPair)
    assert(newAlignmentRecord.getFailedVendorQualityChecks === newSAMRecord.getReadFailsVendorQualityCheckFlag)
    assert(!newAlignmentRecord.getReadMapped === newSAMRecord.getReadUnmappedFlag)
    assert(newAlignmentRecord.getReadName === newSAMRecord.getReadName)
    assert(newAlignmentRecord.getReadNegativeStrand === newSAMRecord.getReadNegativeStrandFlag)

  }

}
