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

import com.google.common.base.Splitter
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.FragmentConverter
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.util.ReferenceFile
import org.bdgenomics.formats.avro._
import scala.collection.JavaConversions._
import scala.math.max

class NucleotideContigFragmentRDDFunctions(rdd: RDD[NucleotideContigFragment]) extends ADAMSequenceDictionaryRDDAggregator[NucleotideContigFragment](rdd) with ReferenceFile {

  /**
   * Converts an RDD of nucleotide contig fragments into reads. Adjacent contig fragments are
   * combined.
   *
   * @return Returns an RDD of reads.
   */
  def toReads: RDD[AlignmentRecord] = {
    FragmentConverter.convertRdd(rdd)
  }

  /**
   * Save nucleotide contig fragments in FASTA format.
   *
   * @param fileName file name
   * @param lineWidth hard wrap FASTA formatted sequence at line width, default 60
   */
  def saveAsFasta(fileName: String, lineWidth: Int = 60) = {

    def isFragment(record: NucleotideContigFragment): Boolean = {
      Option(record.getFragmentNumber).isDefined && Option(record.getNumberOfFragmentsInContig).fold(false)(_ > 1)
    }

    def toFasta(record: NucleotideContigFragment): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(record.getContig.getContigName)
      Option(record.getDescription).foreach(n => sb.append(" ").append(n))
      if (isFragment(record)) {
        sb.append(s" fragment ${record.getFragmentNumber + 1} of ${record.getNumberOfFragmentsInContig}")
      }
      for (line <- Splitter.fixedLength(lineWidth).split(record.getFragmentSequence)) {
        sb.append("\n")
        sb.append(line)
      }
      sb.toString
    }

    rdd.map(toFasta).saveAsTextFile(fileName)
  }

  /**
   * Merge fragments by contig name.
   */
  def mergeFragments(): RDD[NucleotideContigFragment] = {

    def merge(first: NucleotideContigFragment, second: NucleotideContigFragment): NucleotideContigFragment = {
      val merged = NucleotideContigFragment.newBuilder(first)
        .setFragmentNumber(null)
        .setFragmentStartPosition(null)
        .setNumberOfFragmentsInContig(null)
        .setFragmentSequence(first.getFragmentSequence + second.getFragmentSequence)
        .build

      merged
    }

    rdd
      .sortBy(fragment => (fragment.getContig.getContigName, Option(fragment.getFragmentNumber).map(_.toInt).getOrElse(-1)))
      .map(fragment => (fragment.getContig.getContigName, fragment))
      .reduceByKey(merge)
      .values
  }

  /**
   * Added for ReferenceFile trait.
   *
   * @see ReferenceFile.scala
   *      
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  def extract(region: ReferenceRegion): String = getReferenceString(region)

  /**
   * From a set of contigs, returns the base sequence that corresponds to a region of the reference.
   *
   * @throws UnsupportedOperationException Throws exception if query region is not found.
   *
   * @param region Reference region over which to get sequence.
   * @return String of bases corresponding to reference sequence.
   */
  def getReferenceString(region: ReferenceRegion): String = {
    def getString(fragment: (ReferenceRegion, NucleotideContigFragment)): (ReferenceRegion, String) = {
      val trimStart = max(0, region.start - fragment._1.start).toInt
      val trimEnd = max(0, fragment._1.end - region.end).toInt

      val fragmentSequence: String = fragment._2.getFragmentSequence

      val str = fragmentSequence.drop(trimStart)
        .dropRight(trimEnd)
      val reg = new ReferenceRegion(
        fragment._1.referenceName,
        fragment._1.start + trimStart,
        fragment._1.end - trimEnd
      )
      (reg, str)
    }

    def reducePairs(
      kv1: (ReferenceRegion, String),
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
      val refPairRDD: RDD[(ReferenceRegion, String)] = rdd.keyBy(ReferenceRegion(_))
        .filter(kv => kv._1.isDefined)
        .map(kv => (kv._1.get, kv._2))
        .filter(kv => kv._1.overlaps(region))
        .sortByKey()
        .map(kv => getString(kv))

      val pair: (ReferenceRegion, String) = refPairRDD.collect.reduceLeft(reducePairs)
      assert(
        pair._1.compareTo(region) == 0,
        "Merging fragments returned a different region than requested."
      )

      pair._2
    } catch {
      case (uoe: UnsupportedOperationException) =>
        throw new UnsupportedOperationException("Could not find " + region + "in reference RDD.")
    }
  }

  def getSequenceRecords(elem: NucleotideContigFragment): Set[SequenceRecord] = {
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
  def flankAdjacentFragments(
    flankLength: Int,
    optSd: Option[SequenceDictionary] = None): RDD[NucleotideContigFragment] = {
    FlankReferenceFragments(rdd, optSd.getOrElse(getSequenceDictionary(performLexSort = false)), flankLength)
  }

  /**
   * Counts the k-mers contained in a FASTA contig.
   *
   * @param kmerLength The length of k-mers to count.
   * @param optSd An optional sequence dictionary. If none is provided, we recompute the
   *              sequence dictionary on the fly. Default is None.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(
    kmerLength: Int,
    optSd: Option[SequenceDictionary] = None): RDD[(String, Long)] = {
    flankAdjacentFragments(kmerLength, optSd).flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getFragmentSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }
}
