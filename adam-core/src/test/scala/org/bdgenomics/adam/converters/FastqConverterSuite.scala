package org.bdgenomics.adam.converters

import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

/**
  * Created by zyxue on 2016-09-27.
  */
class FastqConverterSuite extends FunSuite {
  val converter = new FastqRecordConverter

  test("testing convert fastq record pair to AlignmentRecord iterator") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val alignRecIterator = converter.convertPair(input)

    assert(alignRecIterator.map(a => a.getReadName) === List("read/1", "read/2"))
    assert(alignRecIterator.map(a => a.getSequence) === List("ATCGA", "TCGAT"))
    assert(alignRecIterator.map(a => a.getQual) === List("abcde", "12345"))
    assert(alignRecIterator.map(a => a.getReadPaired) === List(true, true))
    assert(alignRecIterator.map(a => a.getProperPair) === List(true, true))
    assert(alignRecIterator.map(a => a.getReadInFragment) === List(0, 1))
    assert(alignRecIterator.map(a => a.getReadNegativeStrand) === List(null, null))
    assert(alignRecIterator.map(a => a.getMateNegativeStrand) === List(null, null))
    assert(alignRecIterator.map(a => a.getPrimaryAlignment) === List(null, null))
    assert(alignRecIterator.map(a => a.getSecondaryAlignment) === List(null, null))
    assert(alignRecIterator.map(a => a.getSupplementaryAlignment) === List(null, null))
  }

  test("testing FastqRecordConverter.convertPair with 7-line invalid input") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+"))
    intercept[IllegalArgumentException]{
      val alignRecIterator = converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertPair with invalid input: first read length and qual don't match") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcd\n@read/2\nTCGAT\n+\n12345"))
    intercept[IllegalArgumentException]{
      val alignRecIterator = converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertPair with invalid input: second read length and qual don't match") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n1234"))
    intercept[IllegalArgumentException]{
      val alignRecIterator = converter.convertPair(input)
    }
  }
}
