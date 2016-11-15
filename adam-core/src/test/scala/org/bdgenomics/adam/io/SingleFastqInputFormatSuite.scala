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
package org.bdgenomics.adam.io

import org.bdgenomics.adam.util.ADAMFunSuite
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text

class SingleFastqInputFormatSuite extends ADAMFunSuite {
  (1 to 4) foreach { testNumber =>
    val inputName = "fastq_sample%d.fq".format(testNumber)
    val expectedOutputName = "single_" + inputName + ".output"
    val expectedOutputPath = testFile(expectedOutputName)
    val expectedOutputData = scala.io.Source.fromFile(expectedOutputPath).mkString

    sparkTest("FASTQ hadoop reader: %s->%s".format(inputName, expectedOutputName)) {
      def ifq_reader: RDD[(Void, Text)] = {
        val path = testFile(inputName)
        sc.newAPIHadoopFile(path,
          classOf[SingleFastqInputFormat],
          classOf[Void],
          classOf[Text])
      }

      val ifq_reads = ifq_reader.collect()

      val testOutput = new StringBuilder()

      ifq_reads.foreach(pair => {
        testOutput.append(">>>fastq record start>>>\n")
        testOutput.append(pair._2)
        testOutput.append("<<<fastq record end<<<\n")
      })

      assert(testOutput.toString() == expectedOutputData)
    }
  }
}

