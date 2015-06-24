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
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.mapreduce.{ OutputFormat => NewOutputFormat, _ }
import org.apache.spark.Logging
import org.apache.spark.rdd.{ InstrumentedOutputFormat, RDD }
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.utils.misc.HadoopUtil
import org.apache.parquet.avro.AvroParquetOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.ContextUtil

trait ADAMSaveAnyArgs extends SaveArgs {
  var sortFastqOutput: Boolean
}

class ADAMRDDFunctions[T <% IndexedRecord: Manifest](rdd: RDD[T]) extends Serializable with Logging {

  def adamParquetSave(args: SaveArgs): Unit = {
    adamParquetSave(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding
    )
  }

  def adamParquetSave(filePath: String,
                      blockSize: Int = 128 * 1024 * 1024,
                      pageSize: Int = 1 * 1024 * 1024,
                      compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                      disableDictionaryEncoding: Boolean = false,
                      schema: Option[Schema] = None): Unit = SaveAsADAM.time {
    log.info("Saving data in ADAM format")

    val job = HadoopUtil.newJob(rdd.context)
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job,
      if (schema.isDefined) schema.get
      else manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[InstrumentedADAMAvroParquetOutputFormat],
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
class ADAMSpecificRecordSequenceDictionaryRDDAggregator[T <% IndexedRecord: Manifest](rdd: RDD[T])
    extends ADAMSequenceDictionaryRDDAggregator[T](rdd) {

  def getSequenceRecordsFromElement(elem: T): Set[SequenceRecord] = {
    Set(SequenceRecord.fromSpecificRecord(elem))
  }
}

class InstrumentedADAMAvroParquetOutputFormat extends InstrumentedOutputFormat[Void, IndexedRecord] {
  override def outputFormatClass(): Class[_ <: NewOutputFormat[Void, IndexedRecord]] = classOf[AvroParquetOutputFormat]
  override def timerName(): String = WriteADAMRecord.timerName
}
