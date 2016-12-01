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
package org.bdgenomics.adam.apis.java

import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class JavaADAMContextSuite extends ADAMFunSuite {

  sparkTest("ctr is accessible") {
    new JavaADAMContext(new ADAMContext(sc))
  }

  sparkTest("can read and write a small .SAM file") {
    val path = copyResource("small.sam")
    val aRdd = sc.loadAlignments(path)
    assert(aRdd.jrdd.count() === 20)

    val newRdd = JavaADAMReadConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 20)
  }

  sparkTest("can read and write a small FASTA file") {
    val path = copyResource("chr20.250k.fa.gz")
    val aRdd = sc.loadSequences(path)
    assert(aRdd.jrdd.count() === 26)

    val newRdd = JavaADAMContigConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 26)
  }

  sparkTest("can read and write a small .SAM file as fragments") {
    val path = copyResource("small.sam")
    val aRdd = sc.loadFragments(path)
    assert(aRdd.jrdd.count() === 20)

    val newRdd = JavaADAMFragmentConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 20)
  }

  sparkTest("can read and write a small .bed file as features") {
    val path = copyResource("gencode.v7.annotation.trunc10.bed")
    val aRdd = sc.loadFeatures(path)
    assert(aRdd.jrdd.count() === 10)

    val newRdd = JavaADAMFeatureConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 10)
  }

  sparkTest("can read and write a small .vcf as genotypes") {
    val path = copyResource("small.vcf")
    val aRdd = sc.loadGenotypes(path)
    assert(aRdd.jrdd.count() === 18)

    val newRdd = JavaADAMGenotypeConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 18)
  }

  sparkTest("can read and write a small .vcf as variants") {
    val path = copyResource("small.vcf")
    val aRdd = sc.loadVariants(path)
    assert(aRdd.jrdd.count() === 6)

    val newRdd = JavaADAMVariantConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 6)
  }
}
