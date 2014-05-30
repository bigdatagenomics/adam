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
package org.bdgenomics.adam.parquet_reimpl {

  import org.apache.avro.Schema
  import org.apache.avro.generic.IndexedRecord
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{ Partition, SparkContext, TaskContext }
  import org.bdgenomics.adam.io.{ ByteAccess, FileLocator }
  import org.bdgenomics.adam.parquet_reimpl.filters.CombinedFilter
  import org.bdgenomics.adam.parquet_reimpl.index._
  import org.bdgenomics.adam.rdd._
  import parquet.avro.{ AvroSchemaConverter, UsableAvroRecordMaterializer }
  import parquet.filter.UnboundRecordFilter
  import parquet.schema.MessageType
  import scala.reflect._

  /**
   * AvroIndexedParquetRDD is an RDD which reads an index file, and (for a given CombinedFilter,
   * which contains a predicate on the entries of the index) has as many partitions as there are
   * unique row groups within the indexed Parquet files whose index entries satisfy the predicate.
   *
   * So, for example: if you had two Parquet files, with two row groups each, and the CombinedFilter's
   * index entry predicate matched index entries corresponding to both row groups of the first file and
   * only one row group of the second, you'd have an RDD with three partitions total that read from
   * both parquet files to materialize its records.
   *
   * @param sc The SparkContext for this RDD
   * @param filter The CombinedFilter for the records in this RDD -- note that this determines _both_
   *               which records are in the RDD as well as how many partitions the RDD has, see the
   *               notes on the semantics of CombinedFilter in the comments for that class.
   * @param indexLocator The FileLocator that locates the index file itself
   * @param dataRootLocator The index file contains paths to Parquet files; these paths are relative to
   *                        this value, the dataRootLocator.  (This makes it easier to write tests for
   *                        indexing and for this RDD).
   * @param requestedSchema The user-defined projection for the records of this RDD
   * @tparam T the type of the record in this RDD.
   */
  class AvroIndexedParquetRDD[T <: IndexedRecord: ClassTag](@transient sc: SparkContext,
                                                            private val filter: CombinedFilter[T, IDRangeIndexEntry],
                                                            private val indexLocator: FileLocator,
                                                            private val dataRootLocator: FileLocator,
                                                            @transient private val requestedSchema: Option[Schema])

      extends RDD[T](sc, Nil) {

    override protected def getPartitions: Array[Partition] = {
      val index: IDRangeIndex = new IDRangeIndex(indexLocator.bytes)

      /**
       * There are three steps here:
       *
       * 1. find the relevant index entries from the index
       * 2. find the distinct parquet files from the corresponding entries, and load their
       *    metadata.
       * 3. use the metadata of the parquet files to construct the corresponding ParquetPartition
       *    objects.
       */

      /*
    Step 1: find the relevant index entries
     */
      val entries: Iterable[IDRangeIndexEntry] = index.findIndexEntries(filter.indexPredicate)

      /*
    Step 2: get the parquet file metadata.
     */
      val parquetFiles: Map[String, ParquetFileMetadata] = entries.map(_.path).toSeq.distinct.map {
        case parquetFilePath: String =>
          val parquetLocator = dataRootLocator.relativeLocator(parquetFilePath)
          parquetFilePath -> AvroParquetFileMetadata(parquetLocator, requestedSchema)
      }.toMap

      /*
    Step 3: build the ParquetPartition values.
     */
      entries.toArray.map {
        case IDRangeIndexEntry(path, i, sample, range) => parquetFiles(path).partition(i)
      }
    }

    override def compute(split: Partition, context: TaskContext): Iterator[T] = {
      val reqSchema = classTag[T].runtimeClass.newInstance().asInstanceOf[T].getSchema
      val parquetPartition = split.asInstanceOf[ParquetPartition]
      val byteAccess = parquetPartition.locator.bytes
      def requestedMessageType = parquetPartition.requestedSchema.convertToParquet()

      val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, reqSchema)

      parquetPartition.materializeRecords(byteAccess, avroRecordMaterializer, filter.recordFilter)
    }
  }

  class AvroParquetRDD[T <: IndexedRecord: ClassTag](@transient sc: SparkContext,
                                                     private val filter: UnboundRecordFilter,
                                                     private val parquetFile: FileLocator,
                                                     @transient private val requestedSchema: Option[Schema])
      extends RDD[T](sc, Nil) {

    assert(requestedSchema != null, "Use \"None\" instead of null for no schema.")

    def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
      schema match {
        case None    => fileMessageType
        case Some(s) => new AvroSchemaConverter().convert(s)
      }

    def io(): ByteAccess = parquetFile.bytes

    override protected def getPartitions: Array[Partition] = {

      val fileMetadata = ParquetCommon.readFileMetadata(io())
      val footer = new Footer(fileMetadata)
      val fileMessageType = ParquetCommon.parseMessageType(fileMetadata)
      val fileSchema = new ParquetSchemaType(fileMessageType)
      val requestedMessage = convertAvroSchema(requestedSchema, fileMessageType)
      val requested = new ParquetSchemaType(requestedMessage)

      footer.rowGroups.zipWithIndex.map {
        case (rg, i) => new ParquetPartition(parquetFile, i, rg, requested, fileSchema)
      }.toArray
    }

    override def compute(split: Partition, context: TaskContext): Iterator[T] = {
      val reqSchema = classTag[T].runtimeClass.newInstance().asInstanceOf[T].getSchema
      val byteAccess = io()
      val parquetPartition = split.asInstanceOf[ParquetPartition]
      def requestedMessageType = parquetPartition.requestedSchema.convertToParquet()

      val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, reqSchema)

      parquetPartition.materializeRecords(byteAccess, avroRecordMaterializer, filter)
    }
  }
}

package parquet.avro {

  import org.apache.avro.Schema
  import parquet.io.api.{ GroupConverter, RecordMaterializer }
  import parquet.schema.MessageType

  /**
   * Once again, Parquet has put AvroRecordMaterializer and (sigh) AvroIndexedRecordConverter
   * as package-private classes in parquet.avro.  Therefore, we need to write this wrapper
   * and place it in the parquet.avro package directly, so that we have access to the appropriate
   * methods.
   */
  class UsableAvroRecordMaterializer[T <: org.apache.avro.generic.IndexedRecord](root: AvroIndexedRecordConverter[T]) extends RecordMaterializer[T] {
    def this(requestedSchema: MessageType, avroSchema: Schema) =
      this(new AvroIndexedRecordConverter[T](requestedSchema, avroSchema))

    def getCurrentRecord: T = root.getCurrentRecord
    def getRootConverter: GroupConverter = root
  }
}
