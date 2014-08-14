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
package org.bdgenomics.adam.rdd

import java.util.logging.Level
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.pileup.PileupAggregator
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

class ADAMRDDFunctions[T <% SpecificRecord: Manifest](rdd: RDD[T]) extends Serializable {

  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false) {
    val job = HadoopUtil.newJob(rdd.context)
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[AvroParquetOutputFormat],
      ContextUtil.getConfiguration(job))
  }

}

/**
 * A class that provides functions to recover a sequence dictionary from a generic RDD of records.
 *
 * @tparam T Type contained in this RDD.
 * @param rdd RDD over which aggregation is supported.
 */
abstract class ADAMSequenceDictionaryRDDAggregator[T](rdd: RDD[T]) extends Serializable with Logging {
  /**
   * For a single RDD element, returns 0+ sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecordsFromElement(elem: T): scala.collection.Set[SequenceRecord]

  /**
   * Aggregates together a sequence dictionary from the different individual reference sequences
   * used in this dataset.
   *
   * @return A sequence dictionary describing the reference contigs in this dataset.
   */
  def adamGetSequenceDictionary(): SequenceDictionary = {
    def mergeRecords(l: List[SequenceRecord], rec: T): List[SequenceRecord] = {
      val recs = getSequenceRecordsFromElement(rec)

      recs.foldLeft(l)((li: List[SequenceRecord], r: SequenceRecord) => {
        if (!li.contains(r)) {
          r :: li
        } else {
          li
        }
      })
    }

    def foldIterator(iter: Iterator[T]): SequenceDictionary = {
      val recs = iter.foldLeft(List[SequenceRecord]())(mergeRecords)
      SequenceDictionary(recs: _*)
    }

    rdd.mapPartitions(iter => Iterator(foldIterator(iter)), preservesPartitioning = true)
      .reduce(_ ++ _)
  }

}

/**
 * A class that provides functions to recover a sequence dictionary from a generic RDD of records
 * that are defined in Avro. This class assumes that the reference identification fields are
 * defined inside of the given type.
 *
 * @note Avro classes that have specific constraints around sequence dictionary contents should
 * not use this class. Examples include ADAMRecords and ADAMNucleotideContigs
 *
 * @tparam T A type defined in Avro that contains the reference identification fields.
 * @param rdd RDD over which aggregation is supported.
 */
class ADAMSpecificRecordSequenceDictionaryRDDAggregator[T <% SpecificRecord: Manifest](rdd: RDD[T])
    extends ADAMSequenceDictionaryRDDAggregator[T](rdd) {

  def getSequenceRecordsFromElement(elem: T): Set[SequenceRecord] = {
    Set(SequenceRecord.fromSpecificRecord(elem))
  }
}

class PileupRDDFunctions(rdd: RDD[Pileup]) extends Serializable with Logging {
  /**
   * Aggregates pileup bases together.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with aggregated bases.
   *
   * @see RodRDDFunctions#adamAggregateRods
   */
  def adamAggregatePileups(coverage: Int = 30): RDD[Pileup] = {
    val helper = new PileupAggregator
    helper.aggregate(rdd, coverage)
  }

  /**
   * Converts ungrouped pileup bases into reference grouped bases.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with rods grouped by reference position.
   */
  def adamPileupsToRods(coverage: Int = 30): RDD[Rod] = {
    val groups = rdd.groupBy((p: Pileup) => ReferencePosition(p), coverage)

    groups.map(kv => Rod(kv._1, kv._2.toList))
  }
}

class RodRDDFunctions(rdd: RDD[Rod]) extends Serializable with Logging {
  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns a flat RDD.
   *
   * @return Rods split up by samples and _not_ grouped together.
   */
  def adamSplitRodsBySamples(): RDD[Rod] = {
    rdd.flatMap(_.splitBySamples())
  }

  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns an RDD where the samples are grouped by the reference position.
   *
   * @return Rods split up by samples and grouped together by position.
   */
  def adamDivideRodsBySamples(): RDD[(ReferencePosition, List[Rod])] = {
    rdd.keyBy(_.position).map(r => (r._1, r._2.splitBySamples()))
  }

  /**
   * Inside of a rod, aggregates pileup bases together.
   *
   * @return RDD with aggregated rods.
   *
   * @see ADAMPileupRDDFunctions#adamAggregatePileups
   */
  def adamAggregateRods(): RDD[Rod] = {
    val helper = new PileupAggregator
    rdd.map(r => (r.position, r.pileups))
      .map(kv => (kv._1, helper.flatten(kv._2)))
      .map(kv => new Rod(kv._1, kv._2))
  }

  /**
   * Returns the average coverage for all pileups.
   *
   * @note Coverage value does not include locus positions where no reads are mapped, as no rods exist for these positions.
   * @note If running on an RDD with multiple samples where the rods have been split by sample, will return the average
   *       coverage per sample, _averaged_ over all samples. If the RDD contains multiple samples and the rods have _not_ been split,
   *       this will return the average coverage per sample, _summed_ over all samples.
   *
   * @return Average coverage across mapped loci.
   */
  def adamRodCoverage(): Double = {
    val totalBases: Long = rdd.map(_.pileups.length.toLong).reduce(_ + _)

    // coverage is the total count of bases, over the total number of loci
    totalBases.toDouble / rdd.count().toDouble
  }
}
