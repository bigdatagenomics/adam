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
package org.bdgenomics.adam.api.java

import htsjdk.samtools.ValidationStringency
import java.util.ArrayList
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.ds.ADAMContext
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class JavaADAMContextSuite extends ADAMFunSuite {

  def jac: JavaADAMContext = new JavaADAMContext(new ADAMContext(sc))

  sparkTest("can read and write a small .SAM file") {
    val path = copyResource("small.sam")
    val aRdd = jac.loadAlignments(path)
    assert(aRdd.jrdd.count() === 20)

    val newRdd = JavaADAMReadConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 20)
  }

  sparkTest("loadIndexedBam with multiple ReferenceRegions") {
    val refRegion1 = ReferenceRegion("chr2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val path = testFile("indexed_bams/sorted.bam")
    val rrList = new ArrayList[ReferenceRegion](2)
    rrList.add(refRegion1)
    rrList.add(refRegion2)
    val reads = jac.loadIndexedBam(path, rrList, ValidationStringency.STRICT)
    assert(reads.rdd.count == 2)
  }

  sparkTest("can read and write a small .SAM file as fragments") {
    val path = copyResource("small.sam")
    val aRdd = jac.loadFragments(path)
    assert(aRdd.jrdd.count() === 20)

    val newRdd = JavaADAMFragmentConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 20)
  }

  sparkTest("can read and write a small .bed file as features") {
    val path = copyResource("gencode.v7.annotation.trunc10.bed")
    val aRdd = jac.loadFeatures(path)
    assert(aRdd.jrdd.count() === 10)

    val newRdd = JavaADAMFeatureConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 10)
  }

  sparkTest("can read and write a small .bed file as coverage") {
    val path = copyResource("sample_coverage.bed")
    val aRdd = jac.loadCoverage(path)
    assert(aRdd.jrdd.count() === 3)

    val newRdd = JavaADAMCoverageConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 3)
  }

  sparkTest("can read and write a small .vcf as genotypes") {
    val path = copyResource("small.vcf")
    val aRdd = jac.loadGenotypes(path)
    assert(aRdd.jrdd.count() === 18)

    val newRdd = JavaADAMGenotypeConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 18)
  }

  sparkTest("can read and write a small .vcf as variants") {
    val path = copyResource("small.vcf")
    val aRdd = jac.loadVariants(path)
    assert(aRdd.jrdd.count() === 6)

    val newRdd = JavaADAMVariantConduit.conduit(aRdd, sc)

    assert(newRdd.jrdd.count() === 6)
  }

  sparkTest("can read a two bit file") {
    val path = copyResource("hg19.chrM.2bit")
    val refFile = jac.loadReferenceFile(path)
    assert(refFile.extract(ReferenceRegion("hg19_chrM", 16561, 16571)) === "CATCACGATG")
  }

  sparkTest("can read and write .fa as sequences") {
    val path = copyResource("trinity.fa")
    val sequences = jac.loadDnaSequences(path)
    assert(sequences.jrdd.count() === 5)

    val newRdd = JavaADAMSequenceConduit.conduit(sequences, sc)

    assert(newRdd.jrdd.count() === 5)
  }

  sparkTest("can read and write .fa as slices") {
    val path = copyResource("trinity.fa")
    val slices = jac.loadSlices(path, 10000L)
    assert(slices.jrdd.count() === 5)

    val newRdd = JavaADAMSliceConduit.conduit(slices, sc)

    assert(newRdd.jrdd.count() === 5)
  }
}
