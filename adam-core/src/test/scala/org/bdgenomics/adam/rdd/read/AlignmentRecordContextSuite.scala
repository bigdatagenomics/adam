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
package org.bdgenomics.adam.rdd.read

import java.io.File
import org.apache.hadoop.fs.Path
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class ADAMAlignmentRecordContextSuite extends ADAMFunSuite {

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.fq".format(testNumber)
    val path = ClassLoader.getSystemClassLoader.getResource(inputName).getFile

    sparkTest("import records from interleaved FASTQ: %d".format(testNumber)) {

      val reads = AlignmentRecordContext.adamInterleavedFastqLoad(sc, path)
      if (testNumber == 1) {
        assert(reads.count === 6)
        assert(reads.filter(_.getReadPaired).count === 6)
        assert(reads.filter(_.getFirstOfPair).count === 3)
        assert(reads.filter(_.getSecondOfPair).count === 3)
      } else {
        assert(reads.count === 4)
        assert(reads.filter(_.getReadPaired).count === 4)
        assert(reads.filter(_.getFirstOfPair).count === 2)
        assert(reads.filter(_.getSecondOfPair).count === 2)
      }

      assert(reads.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.collect.forall(_.getQual.toString.length === 250))
    }
  }
  sparkTest("import records from interleaved multiline FASTQ") {
    val path = ClassLoader.getSystemClassLoader.getResource("interleaved_multiline_fastq.fq").getFile
    val reads = AlignmentRecordContext.adamInterleavedFastqLoad(sc, path)

    assert(reads.count === 6)
    assert(reads.filter(_.getReadPaired).count === 6)
    assert(reads.filter(_.getFirstOfPair).count === 3)
    assert(reads.filter(_.getSecondOfPair).count === 3)
    assert(reads.collect.forall(_.getSequence.toString.length === 250))
    assert(reads.collect.forall(_.getQual.toString.length === 250))
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.fq".format(testNumber)
    val path = ClassLoader.getSystemClassLoader.getResource(inputName).getFile

    sparkTest("import records from single ended FASTQ: %d".format(testNumber)) {

      val reads = AlignmentRecordContext.adamUnpairedFastqLoad(sc, path)
      if (testNumber == 1) {
        assert(reads.count === 6)
        assert(reads.filter(_.getReadPaired).count === 0)
      } else if (testNumber == 4) {
        assert(reads.count === 4)
        assert(reads.filter(_.getReadPaired).count === 0)
      } else {
        assert(reads.count === 5)
        assert(reads.filter(_.getReadPaired).count === 0)
      }

      assert(reads.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.collect.forall(_.getQual.toString.length === 250))
    }
  }
  sparkTest("import records from single ended multiline FASTQ") {
    val path = ClassLoader.getSystemClassLoader.getResource("interleaved_multiline_fastq.fq").getFile
    val reads = AlignmentRecordContext.adamUnpairedFastqLoad(sc, path)

    assert(reads.count === 6, "")
    assert(reads.filter(_.getReadPaired).count === 0)
    assert(reads.collect.forall(_.getSequence.toString.length === 250))
    assert(reads.collect.forall(_.getQual.toString.length === 250))
  }
}

