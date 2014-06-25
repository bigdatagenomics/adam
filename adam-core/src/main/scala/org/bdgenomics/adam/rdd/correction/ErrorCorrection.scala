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
package org.bdgenomics.adam.rdd.correction

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.PhredUtils

private[rdd] object ErrorCorrection extends Logging {

  val ec = new ErrorCorrection

  /**
   * Cuts reads into _q_-mers, and then finds the _q_-mer weight. Q-mers are described in:
   *
   * Kelley, David R., Michael C. Schatz, and Steven L. Salzberg. "Quake: quality-aware detection
   * and correction of sequencing errors." Genome Biol 11.11 (2010): R116.
   *
   * _Q_-mers are _k_-mers weighted by the quality score of the bases in the _k_-mer.
   *
   * @param rdd RDD to count q-mers on.
   * @param qmerLength The value of _q_ to use for cutting _q_-mers.
   * @return Returns an RDD containing q-mer/weight pairs.
   */
  def countQmers(rdd: RDD[ADAMRecord],
                 qmerLength: Int): RDD[(String, Double)] = {
    // generate qmer counts
    rdd.flatMap(ec.readToQmers(_, qmerLength))
      .reduceByKey(_ + _)
  }
}

private[correction] class ErrorCorrection extends Serializable with Logging {

  /**
   * Cuts a single read into q-mers.
   *
   * @param read Read to cut.
   * @param qmerLength The length of the qmer to cut.
   * @return Returns an iterator containing q-mer/weight mappings.
   */
  def readToQmers(read: ADAMRecord,
                  qmerLength: Int = 20): Iterator[(String, Double)] = {
    // get read bases and quality scores
    val bases = read.getSequence.toSeq
    val scores = read.getQual.toString.toCharArray.map(q => {
      PhredUtils.phredToSuccessProbability(q - 33)
    })

    // zip and put into sliding windows to get qmers
    bases.zip(scores)
      .sliding(qmerLength)
      .map(w => {
        // bases are first in tuple
        val b = w.map(_._1)

        // quals are second
        val q = w.map(_._2)

        // reduce bases into string, reduce quality scores
        (b.map(_.toString).reduce(_ + _), q.reduce(_ * _))
      })
  }

}
