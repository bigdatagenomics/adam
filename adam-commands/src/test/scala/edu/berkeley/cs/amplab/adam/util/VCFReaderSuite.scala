/**
 * User: tdanford
 * Date: 11/29/13
 */
package edu.berkeley.cs.amplab.adam.util

import org.scalatest._
import java.io.File

class VCFReaderSuite extends FunSuite {

  test("Correctly reads a seven-line VCF from the 1k genomes project") {
    val file : File = new File(ClassLoader.getSystemClassLoader.getResource("seven-line-correct-test.vcf").getFile)

    val count = VCFReader(file).count( call => true )
    assert(count === 1361)

    val call = VCFReader(file).find( call => call.getSampleId == "HG00096" && call.getStart.toInt == 69270).get
    assert( call.getReferenceAllele === "A" )
    assert( call.getStart === 69270 )
    assert( call.getSampleId === "HG00096" )
    assert( call.getAllele1 === "G" )
    assert( call.getAllele2 === "G" )
  }
}
