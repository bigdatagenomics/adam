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
import org.bdgenomics.adam.converters.FragmentConverter
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.HadoopUtil
import org.apache.parquet.avro.AvroParquetOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.ContextUtil
import scala.math.max
import scala.Some

class NucleotideContigFragmentRDDFunctions(rdd: RDD[NucleotideContigFragment]) extends ADAMSequenceDictionaryRDDAggregator[NucleotideContigFragment](rdd) {

  /**
   * Converts an RDD of nucleotide contig fragments into reads. Adjacent contig fragments are
   * combined.
   *
   * @return Returns an RDD of reads.
   */
  def toReads(): RDD[AlignmentRecord] = {
    FragmentConverter.convertRdd(rdd)
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

      (kv1._1.merge(kv2._1), if (kv1._1.compareTo(kv2._1) <= 0) {
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

      assert(pair._1.compareTo(region) == 0,
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

  /**
   * For all adjacent records in the RDD, we extend the records so that the adjacent
   * records now overlap by _n_ bases, where _n_ is the flank length.
   *
   * @param flankLength The length to extend adjacent records by.
   * @param optSd An optional sequence dictionary. If none is provided, we recompute the
   *              sequence dictionary on the fly. Default is None.
   * @return Returns the RDD, with all adjacent fragments extended with flanking sequence.
   */
  def flankAdjacentFragments(flankLength: Int,
                             optSd: Option[SequenceDictionary] = None): RDD[NucleotideContigFragment] = {
    FlankReferenceFragments(rdd, optSd.getOrElse(adamGetSequenceDictionary(performLexSort = false)), flankLength)
  }

  /**
   * Counts the k-mers contained in a FASTA contig.
   *
   * @param kmerLength The length of k-mers to count.
   * @param optSd An optional sequence dictionary. If none is provided, we recompute the
   *              sequence dictionary on the fly. Default is None.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(kmerLength: Int,
                 optSd: Option[SequenceDictionary] = None): RDD[(String, Long)] = {
    flankAdjacentFragments(kmerLength, optSd).flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getFragmentSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }
}
