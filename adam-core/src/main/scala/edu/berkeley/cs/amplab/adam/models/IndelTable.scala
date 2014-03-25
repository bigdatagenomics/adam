/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, ADAMVariant}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class IndelTable (private val table: Map[Int, Seq[Consensus]]) extends Serializable with Logging {
  log.info("Indel table has %s contigs and %s entries".format(table.size, 
                                                              table.values.map(_.size).sum))

  /**
   * Returns all known indels within the given reference region. If none are known, returns an empty Seq.
   *
   * @param region Region to look for known indels.
   * @return Returns a sequence of consensuses.
   */
  def getIndelsInRegion(region: ReferenceRegion): Seq[Consensus] = {
    if (table.contains(region.refId)) {
      val bucket = table(region.refId)
      
      bucket.filter(_.index.overlaps(region))
    } else {
      Seq()
    }
  }
}

object IndelTable {

  /**
   * Creates an indel table from a file containing known indels.
   *
   * @param knownIndelsFile Path to file with known indels.
   * @param sc SparkContext to use for loading.
   * @return Returns a table with the known indels populated.
   */
  def apply(knownIndelsFile: String, sc: SparkContext): IndelTable = {
    val rdd: RDD[ADAMVariantContext] = sc.adamVCFLoad(knownIndelsFile)
    apply(rdd.map(_.variant.variant))
  }

  /**
   * Creates an indel table from an RDD containing known variants.
   *
   * @param variants RDD of variants.
   * @return Returns a table with known indels populated.
   */
  def apply(variants: RDD[ADAMVariant]): IndelTable = {
    val consensus: Map[Int, Seq[Consensus]] = variants.filter(v => v.getReferenceAllele.length != v.getVariantAllele.length)
      .map(v => {
        val refId: Int = v.getContig.getContigId
        val consensus = if (v.getReferenceAllele.length > v.getVariantAllele.length) {
          // deletion
          val deletionLength = v.getReferenceAllele.length - v.getVariantAllele.length
          val start = v.getPosition + v.getVariantAllele.length
          
          Consensus("", ReferenceRegion(refId, start, start + deletionLength))
        } else {
          // insertion
          val insertionLength = v.getVariantAllele.length - v.getReferenceAllele.length
          val start = v.getPosition + v.getReferenceAllele.length
          
          Consensus(v.getVariantAllele.toString.drop(v.getReferenceAllele.length), ReferenceRegion(refId, start, start + 1))
        }

        (refId, consensus)
      }).groupByKey()
        .collect
        .toMap

    new IndelTable(consensus)
  }
}
