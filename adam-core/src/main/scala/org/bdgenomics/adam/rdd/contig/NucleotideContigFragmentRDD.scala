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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.FragmentConverter
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.AvroGenomicRDD
import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment }

case class NucleotideContigFragmentRDD(rdd: RDD[NucleotideContigFragment],
                                       sequences: SequenceDictionary) extends AvroGenomicRDD[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  /**
   * Converts an RDD of nucleotide contig fragments into reads. Adjacent contig fragments are
   * combined.
   *
   * @return Returns an RDD of reads.
   */
  def toReads: RDD[AlignmentRecord] = {
    FragmentConverter.convertRdd(rdd)
  }

  protected def replaceRdd(newRdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    copy(rdd = newRdd)
  }
}
