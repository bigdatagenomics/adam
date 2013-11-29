/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.commands

import org.scalatest._
import edu.berkeley.cs.amplab.adam.avro.VariantType

class Vcf2AdamSuite extends FunSuite {

  test("VcfConverter.getType() is able to return all indicates VariantType values") {

    import VcfConverter._

    assert( getType("A", "T") === VariantType.SNP )
    assert( getType("AA", "AT") === VariantType.MNP )
    assert( getType("A", "AA") === VariantType.Insertion )
    assert( getType("AA", "A") === VariantType.Deletion )
    assert( getType("AAA", "AA,ATGC") === VariantType.Complex )
    assert( getType("A", "<AA") === VariantType.SV )
  }
}
