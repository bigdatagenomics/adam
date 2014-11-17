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

import org.bdgenomics.adam.rdd.ADAMContext._
import htsjdk.samtools.{ SAMFileReader, SAMSequenceRecord, SAMSequenceDictionary }
import org.scalatest.FunSuite
import java.io.File

class SequenceDictionarySuite extends FunSuite {
  test("Convert from sam sequence record and back") {
    val sr = new SAMSequenceRecord("1", 1000)
    sr.setAttribute(SAMSequenceRecord.URI_TAG, "http://bigdatagenomics.github.io/1")

    val asASR: SequenceRecord = SequenceRecord.fromSAMSequenceRecord(sr)

    assert(asASR.name === "1")
    assert(asASR.length === 1000L)
    assert(asASR.url === Some("http://bigdatagenomics.github.io/1"))

    val asPSR: SAMSequenceRecord = SequenceRecord.toSAMSequenceRecord(asASR)

    assert(sr.isSameSequence(asPSR))
  }

  test("Convert from SAM sequence dictionary file (with extra fields)") {
    val path = ClassLoader.getSystemClassLoader.getResource("dict_with_accession.dict").getFile
    val ssd = SAMFileReader.getSequenceDictionary(new File(path))

    val chr1 = ssd.getSequence("1") // Validate that extra fields are parsed
    assert(chr1 != null)
    val refseq = chr1.getAttribute("REFSEQ")
    assert(refseq === "NC_000001.10")

    val asd = SequenceDictionary(ssd)
    assert(asd.containsRefName("1"))
    assert(!asd.containsRefName("2"))
  }

  test("merge into existing dictionary") {
    val path = ClassLoader.getSystemClassLoader.getResource("dict_with_accession.dict").getFile
    val ssd = SAMFileReader.getSequenceDictionary(new File(path))

    val asd = SequenceDictionary(ssd)
    assert(asd.containsRefName("1"))
    val chr1 = asd("1").get

    val myDict = SequenceDictionary(record(chr1.name, chr1.length, md5 = chr1.md5))
    assert(asd.isCompatibleWith(myDict))
    assert(myDict.isCompatibleWith(asd))
  }

  test("Convert from SAM sequence dictionary and back") {
    val path = ClassLoader.getSystemClassLoader.getResource("dict_with_accession.dict").getFile
    val ssd = SAMFileReader.getSequenceDictionary(new File(path))
    val asd = SequenceDictionary(ssd)
    ssd.assertSameDictionary(SequenceDictionary.toSAMSequenceDictionary(asd))
  }

  test("Can retrieve sequence by name") {
    val rec = record("chr1")
    val asd = SequenceDictionary(rec)
    val recFromName = asd(rec.name)
    assert(recFromName === Some(rec))
  }

  test("SequenceDictionary's with same single element are equal") {
    val asd1 = SequenceDictionary(record("chr1"))
    val asd2 = SequenceDictionary(record("chr1"))
    assert(asd1 === asd2)
  }

  test("SequenceDictionary's with same two elements are equals") {
    val asd1 = SequenceDictionary(record("chr1"), record("chr2"))
    val asd2 = SequenceDictionary(record("chr1"), record("chr2"))
    assert(asd1 === asd2)
  }

  test("SequenceDictionary's with different elements are unequal") {
    val asd1 = SequenceDictionary(record("chr1"), record("chr2"))
    val asd2 = SequenceDictionary(record("chr1"), record("chr3"))
    assert(asd1 != asd2)
  }

  test("SequenceDictionaries with same elements in different order are compatible") {
    val asd1 = SequenceDictionary(record("chr1"), record("chr2"))
    val asd2 = SequenceDictionary(record("chr2"), record("chr1"))
    assert(asd1.isCompatibleWith(asd2))
  }

  test("isCompatible tests equality on overlap") {
    val s1 = SequenceDictionary(record("foo"), record("bar"))
    val s2 = SequenceDictionary(record("bar"), record("quux"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))
    val s4 = SequenceDictionary(record("foo", 1001))
    assert(s1.isCompatibleWith(s2))
    assert(s1 isCompatibleWith s3)
    assert(!(s3 isCompatibleWith s4))
  }

  test("The addition + works correctly") {
    val s1 = SequenceDictionary()
    val s2 = SequenceDictionary(record("foo"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))

    assert(s1 + record("foo") === s2)
    assert(s2 + record("foo") === s2)
    assert(s2 + record("bar") === s3)
  }

  test("The append operation ++ works correctly") {
    val s1 = SequenceDictionary()
    val s2a = SequenceDictionary(record("foo"))
    val s2b = SequenceDictionary(record("bar"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))

    assert(s1 ++ s1 === s1)
    assert(s1 ++ s2a === s2a)
    assert(s1 ++ s2b === s2b)
    assert(s2a ++ s2b === s3)
  }

  test("ContainsRefName works correctly for different string types") {
    val dict = SequenceDictionary(record("chr0"),
      record("chr1"),
      record("chr2"),
      record("chr3"))
    val str0: String = "chr0"
    val str1: java.lang.String = "chr1"

    assert(dict.containsRefName(str0))
    assert(dict.containsRefName(str1))
  }

  test("Apply on name works correctly for different String types") {
    val dict = SequenceDictionary(
      record("chr0"),
      record("chr1"),
      record("chr2"),
      record("chr3"))
    val str0: String = "chr0"
    val str1: java.lang.String = "chr1"

    assert(dict(str0).get.name === "chr0")
    assert(dict(str1).get.name === "chr1")
  }

  def record(name: String, length: Long = 1000, url: Option[String] = None, md5: Option[String] = None): SequenceRecord =
    new SequenceRecord(name, length, url = url, md5 = md5)

  test("convert from sam sequence record and back") {
    val sr = new SAMSequenceRecord("chr0", 1000)
    sr.setAttribute(SAMSequenceRecord.URI_TAG, "http://bigdatagenomics.github.io/chr0")

    val conv = SequenceRecord.fromSAMSequenceRecord(sr)

    assert(conv.name === "chr0")
    assert(conv.length === 1000L)
    assert(conv.url.get === "http://bigdatagenomics.github.io/chr0")

    val convSr = conv.toSAMSequenceRecord

    assert(convSr.isSameSequence(sr))
  }

  test("convert from sam sequence dictionary and back") {
    val sr0 = new SAMSequenceRecord("chr0", 1000)

    val srs = List(sr0)

    val ssd = new SAMSequenceDictionary(srs)

    val asd = SequenceDictionary.fromSAMSequenceDictionary(ssd)

    val toSSD = asd.toSAMSequenceDictionary

    toSSD.assertSameDictionary(ssd)
  }

}
