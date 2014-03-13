/**
 * Copyright 2013-2014. Genome Bridge LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.models

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant }
import org.bdgenomics.adam.rdd.ADAMContext._
import net.sf.samtools.{ SAMFileHeader, SAMFileReader, SAMSequenceRecord, SAMSequenceDictionary }
import org.apache.avro.specific.SpecificRecord
import org.broadinstitute.variant.vcf.{ VCFHeader, VCFContigHeaderLine }
import org.scalatest.FunSuite

class SequenceDictionarySuite extends FunSuite {

  test("containsRefName works as expected 1") {
    val rec1 = record("foo")
    val rec2 = record("bar")
    assert(SequenceDictionary(rec1, rec2).containsRefName("foo"))
    assert(SequenceDictionary(rec1, rec2).containsRefName("bar"))
    assert(!SequenceDictionary(rec1, rec2).containsRefName("foo "))
  }

  test("containsRefName works as expected 2") {
    val rec1 = record("foo")
    val rec2 = record("bar")
    val sd = SequenceDictionary(rec1, rec2)
    val names = sd.getReferenceNames()
    assert(names.toList.filter(_ == "foo").length === 1)
    assert(names.toList.filter(_ == "bar").length === 1)
  }

  test("Can retrieve sequence by Name") {
    val rec1 = record("foo")
    val rec2 = record("bar")
    assert(SequenceDictionary(rec1, rec2)(rec1.name) === rec1)
  }

  test("SequenceDictionaries with same single element are equal") {
    assert(
      SequenceDictionary(record("foo")) === SequenceDictionary(record("foo")))
  }

  test("SequenceDictionaries with same two elements are equals") {
    assert(SequenceDictionary(record("foo"), record("bar")) ===
      SequenceDictionary(record("foo"), record("bar")))
  }

  test("SequenceDictionaries with different elements are unequal") {
    assert(SequenceDictionary(record("foo"), record("bar")) !=
      SequenceDictionary(record("foo"), record("quux")))
  }

  test("SequenceDictionaries with same elements in different order are equal") {
    assert(SequenceDictionary(record("foo"), record("bar")) ===
      SequenceDictionary(record("bar"), record("foo")))
  }

  test("isCompatible tests equality on overlap") {
    val s1 = SequenceDictionary(record("foo"), record("bar"))
    val s2 = SequenceDictionary(record("bar"), record("quux"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))
    val s4 = SequenceDictionary(record("foo", 1001))
    assert(s1 isCompatibleWith s2)
    assert(s1 isCompatibleWith s3)
    assert(!(s3 isCompatibleWith s4))
  }

  test("the addition + works correctly") {
    val s1 = SequenceDictionary()
    val s2 = SequenceDictionary(record("foo"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))

    assert(s1 + record("foo") === s2)
    assert(s2 + record("foo") === s2)
    assert(s2 + record("bar") === s3)
  }

  test("the append operation ++ works correctly") {
    val s1 = SequenceDictionary()
    val s2a = SequenceDictionary(record("foo"))
    val s2b = SequenceDictionary(record("bar"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))

    assert(s1 ++ s1 === s1)
    assert(s1 ++ s2a === s2a)
    assert(s1 ++ s2b === s2b)
    assert(s2a ++ s2b === s3)
  }

  test("containsRefName works correctly") {
    val dict = SequenceDictionary(record("chr0"),
      record("chr1"),
      record("chr2"),
      record("chr3"))
    val str0: String = "chr0"
    val str1: java.lang.String = "chr1"
    val str2: CharSequence = "chr2"
    val str3: java.lang.CharSequence = "chr3"

    assert(dict.containsRefName(str0))
    assert(dict.containsRefName(str1))
    assert(dict.containsRefName(str2))
    assert(dict.containsRefName(str3))
  }

  test("apply on name works correctly") {
    val dict = SequenceDictionary(
      record("chr0"),
      record("chr1"),
      record("chr2"),
      record("chr3"))
    val str0: String = "chr0"
    val str1: java.lang.String = "chr1"
    val str2: CharSequence = "chr2"
    val str3: java.lang.CharSequence = "chr3"

    assert(dict(str0).name === "chr0")
    assert(dict(str1).name === "chr1")
    assert(dict(str2).name === "chr2")
    assert(dict(str3).name === "chr3")
  }

  test("get record from variant using specific record") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .setContigLength(1000)
      .setReferenceURL("http://bigdatagenomics.github.io/chr0")
      .build()
    val variant = ADAMVariant.newBuilder()
      .setContig(contig)
      .setReferenceAllele("A")
      .setVariantAllele("T")
      .build()

    val rec = SequenceRecord.fromSpecificRecord(variant)

    assert(rec.name === "chr0")
    assert(rec.length === 1000L)
    assert(rec.url === "http://bigdatagenomics.github.io/chr0")
  }

  test("convert from sam sequence record and back") {
    val sr = new SAMSequenceRecord("chr0", 1000)

    sr.setAssembly("http://bigdatagenomics.github.io/chr0")

    val conv = SequenceRecord.fromSamSequenceRecord(sr)

    assert(conv.name === "chr0")
    assert(conv.length === 1000L)
    assert(conv.url === "http://bigdatagenomics.github.io/chr0")

    val convSr = conv.toSAMSequenceRecord

    assert(convSr.isSameSequence(sr))
  }

  test("convert from sam sequence dictionary") {
    val sr0 = new SAMSequenceRecord("chr0", 1000)
    println(sr0.getSequenceIndex)
    val srs = List(sr0)

    val ssd = new SAMSequenceDictionary(srs)

    val asd = SequenceDictionary.fromSAMSequenceDictionary(ssd)

    assert(asd("chr0").name === "chr0")
  }

  def record(name: String, length: Int = 1000, url: String = null): SequenceRecord =
    SequenceRecord(name, length, url)

}
