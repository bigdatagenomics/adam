/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.commands

import edu.berkeley.cs.amplab.adam.util.{SparkFunSuite, Args4j}

class CompareAdamSuite extends SparkFunSuite {

  sparkTest("Test that reads12.sam and reads21.sam are the same") {
    val reads12 = ClassLoader.getSystemClassLoader.getResource("reads12.sam").getFile
    val reads21 = ClassLoader.getSystemClassLoader.getResource("reads21.sam").getFile

    val (comp1, comp2) =
      CompareAdam.compareADAM(sc, reads12, reads21, CompareAdam.readLocationsMatchPredicate)

    assert(comp1.total === 200)
    assert(comp2.total === 200)
    assert(comp1.unique === 0)
    assert(comp2.unique === 0)
    assert(comp1.matching === 200)
  }
}
