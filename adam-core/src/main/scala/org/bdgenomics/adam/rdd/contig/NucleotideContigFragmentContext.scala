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
package org.bdgenomics.adam.rdd.contig

import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.FastaConverter
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment

object NucleotideContigFragmentContext {
  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext): NucleotideContigFragmentContext = new NucleotideContigFragmentContext(sc)

  // Add methods specific to the ADAMNucleotideContig RDDs
  implicit def rddToContigFragmentRDD(rdd: RDD[NucleotideContigFragment]) = new NucleotideContigFragmentRDDFunctions(rdd)
}

class NucleotideContigFragmentContext(val sc: SparkContext) extends Serializable with Logging {

  def adamSequenceLoad(filePath: String, fragmentLength: Long): RDD[NucleotideContigFragment] = {
    if (filePath.endsWith(".fasta") || filePath.endsWith(".fa")) {
      val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(filePath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text])

      val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

      log.info("Converting FASTA to ADAM.")
      FastaConverter(remapData, fragmentLength)
    } else {
      sc.adamParquetLoad(filePath)
    }
  }
}
