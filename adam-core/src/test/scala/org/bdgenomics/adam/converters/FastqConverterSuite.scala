package org.bdgenomics.adam.converters

import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

/**
  * Created by zyxue on 2016-09-27.
  */
class FastqConverterSuite extends FunSuite {
  val converter = new FastqRecordConverter

  test("testing FastqRecordConverter.convertPair with valid input") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val alignRecIterator = converter.convertPair(input)

    assert(alignRecIterator.map(a => a.getReadName) === List("read", "read"))
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
      converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertPair with invalid input: first read length and qual don't match") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcd\n@read/2\nTCGAT\n+\n12345"))
    intercept[IllegalArgumentException]{
      converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertPair with invalid input: second read length and qual don't match") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n1234"))
    intercept[IllegalArgumentException]{
      converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertFragment with valid input") {
    val input = (null, new Text("@read\nATCGA\n+\nabcde\n@read\nTCGAT\n+\n12345"))
    val fragment = converter.convertFragment(input)
    assert(fragment.getReadName == "read")

    // more detailed testing
    val align1 = fragment.getAlignments.get(0)
    assert(align1.getReadName === "read")
    assert(align1.getSequence === "ATCGA")
    assert(align1.getQual === "abcde")
    assert(align1.getReadPaired === true)
    assert(align1.getProperPair === true)
    assert(align1.getReadInFragment === 0)

    val align2 = fragment.getAlignments.get(1)
    assert(align2.getReadName === "read")
    assert(align2.getSequence === "TCGAT")
    assert(align2.getQual === "12345")
    assert(align2.getReadPaired === true)
    assert(align2.getProperPair === true)
    assert(align2.getReadInFragment === 1)
  }

  test("testing FastqRecordConverter.convertFragment with another valid input having /1, /2 suffixes") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val fragment = converter.convertFragment(input)
    assert(fragment.getReadName == "read")

    // more detailed testing
    val align1 = fragment.getAlignments.get(0)
    assert(align1.getReadName === "read")
    assert(align1.getSequence === "ATCGA")
    assert(align1.getQual === "abcde")
    assert(align1.getReadPaired === true)
    assert(align1.getProperPair === true)
    assert(align1.getReadInFragment === 0)

    val align2 = fragment.getAlignments.get(1)
    assert(align2.getReadName === "read")
    assert(align2.getSequence === "TCGAT")
    assert(align2.getQual === "12345")
    assert(align2.getReadPaired === true)
    assert(align2.getProperPair === true)
    assert(align2.getReadInFragment === 1)
  }

  test("testing FastqRecordConverter.convertFragment with invalid input: different read names") {
    val input = (null, new Text("@nameX\nATCGA\n+\nabcde\n@nameY/2\nTCGAT\n+\n12345"))
    intercept[IllegalArgumentException] {
      converter.convertFragment (input)
    }
  }

}
