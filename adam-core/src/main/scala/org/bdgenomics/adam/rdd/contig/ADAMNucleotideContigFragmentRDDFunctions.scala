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

import java.util.logging.Level
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.util.{
  HadoopUtil,
  ParquetLogger
}
import org.bdgenomics.formats.avro._
import parquet.avro.AvroParquetOutputFormat
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil
import scala.math.max
import scala.Some

class ADAMNucleotideContigFragmentRDDFunctions(rdd: RDD[NucleotideContigFragment]) extends ADAMSequenceDictionaryRDDAggregator[NucleotideContigFragment](rdd) {

  /**
   * Rewrites the contig IDs of a FASTA reference set to match the contig IDs present in a
   * different sequence dictionary. Sequences are matched by name.
   *
   * @note Contigs with names that aren't present in the provided dictionary are filtered out of the RDD.
   *
   * @param sequenceDict A sequence dictionary containing the preferred IDs for the contigs.
   * @return New set of contigs with IDs rewritten.
   */
  def adamRewriteContigIds(sequenceDict: SequenceDictionary): RDD[NucleotideContigFragment] = {
    // broadcast sequence dictionary
    val bcastDict = rdd.context.broadcast(sequenceDict)

    /**
     * Remaps a single contig.
     *
     * @param fragment Contig to remap.
     * @param dictionary A sequence dictionary containing the IDs to use for remapping.
     * @return An option containing the remapped contig if it's sequence name was found in the dictionary.
     */
    def remapContig(fragment: NucleotideContigFragment, dictionary: SequenceDictionary): Option[NucleotideContigFragment] = {
      val name: CharSequence = fragment.getContig.getContigName

      if (dictionary.containsRefName(name)) {
        // NB : this is a no-op in the non-ref-id world. Should we delete it?
        val newFragment = NucleotideContigFragment.newBuilder(fragment)
          .setContig(fragment.getContig)
          .build()
        Some(newFragment)
      } else {
        None
      }
    }

    // remap all contigs
    rdd.flatMap(c => remapContig(c, bcastDict.value))
  }

  /**
   * From a set of contigs, returns the base sequence that corresponds to a region of the reference.
   *
   * @throws UnsupportedOperationException Throws exception if query region is not found.
   *
   * @param region Reference region over which to get sequence.
   * @return String of bases corresponding to reference sequence.
   */
  def adamGetReferenceString(region: ReferenceRegion): String = {
    def getString(fragment: (ReferenceRegion, NucleotideContigFragment)): (ReferenceRegion, String) = {
      val trimStart = max(0, region.start - fragment._1.start).toInt
      val trimEnd = max(0, fragment._1.end - region.end).toInt

      val fragmentSequence: String = fragment._2.getFragmentSequence

      val str = fragmentSequence.drop(trimStart)
        .dropRight(trimEnd)
      val reg = new ReferenceRegion(fragment._1.referenceName,
        fragment._1.start + trimStart,
        fragment._1.end - trimEnd)
      (reg, str)
    }

    def reducePairs(kv1: (ReferenceRegion, String),
                    kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
      assert(kv1._1.isAdjacent(kv2._1), "Regions being joined must be adjacent. For: " +
        kv1 + ", " + kv2)

      (kv1._1.merge(kv2._1), if (kv1._1.compare(kv2._1) <= 0) {
        kv1._2 + kv2._2
      } else {
        kv2._2 + kv1._2
      })
    }

    try {
      val pair: (ReferenceRegion, String) = rdd.keyBy(ReferenceRegion(_))
        .filter(kv => kv._1.isDefined)
        .map(kv => (kv._1.get, kv._2))
        .filter(kv => kv._1.overlaps(region))
        .sortByKey()
        .map(kv => getString(kv))
        .reduce(reducePairs)

      assert(pair._1.compare(region) == 0,
        "Merging fragments returned a different region than requested.")

      pair._2
    } catch {
      case (uoe: UnsupportedOperationException) =>
        throw new UnsupportedOperationException("Could not find " + region + "in reference RDD.")
    }
  }

  def getSequenceRecordsFromElement(elem: NucleotideContigFragment): Set[SequenceRecord] = {
    // variant context contains a single locus
    Set(SequenceRecord.fromADAMContigFragment(elem))
  }
}
