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
package org.bdgenomics.adam.cli

import com.google.common.io.Files
import java.io.File
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.utils.cli._

class ADAM2FastaSuite extends ADAMFunSuite {

  sparkTest("round trip FASTA to nucleotide contig fragments in ADAM format to FASTA") {
    val fastaFile = testFile("contigs.fa")

    val outputDir = Files.createTempDir()
    val outputContigFragmentsFile = outputDir.getAbsolutePath + "/contigs.adam"
    val outputFastaFile = outputDir.getAbsolutePath + "/contigs.fa"

    val args0: Array[String] = Array(fastaFile, outputContigFragmentsFile)
    new Fasta2ADAM(Args4j[Fasta2ADAMArgs](args0)).run(sc)

    val args1: Array[String] = Array(outputContigFragmentsFile, outputFastaFile)
    new ADAM2Fasta(Args4j[ADAM2FastaArgs](args1)).run(sc)

    val fastaLines = scala.io.Source.fromFile(new File(fastaFile)).getLines().toSeq
    val outputFastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(outputFastaLines.length === fastaLines.length)
    outputFastaLines.zip(fastaLines).foreach(kv => assert(kv._1 === kv._2))
  }
}
