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
package org.bdgenomics.adam.parquet_reimpl

import parquet.filter.UnboundRecordFilter
import parquet.io.api.RecordMaterializer
import parquet.io.ColumnIOFactory
import parquet.column.page.{ PageReadStore, PageReader }
import parquet.column.ColumnDescriptor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{ CompressionCodec => HadoopCompressionCodec }
import org.apache.spark.Partition
import org.bdgenomics.adam.rdd._
import org.bdgenomics.adam.rdd.ParquetColumnDescriptor
import org.bdgenomics.adam.rdd.ParquetRowGroup
import org.bdgenomics.adam.io.{ ByteAccess, FileLocator }

/**
 *
 * ParquetPartition is a Partition implementation, used in our RDD implementations, which corresponds to a
 * single row group in a single Parquet file.
 *
 * @param locator The locator of the Parquet file
 * @param index An integer (assigned by the RDD) for uniqueness
 * @param rowGroup The metadata of the row group in the parquet file which this partition names
 * @param requestedSchema The schema of the object corresponding to the user's projection
 * @param actualSchema The actual schema of the objects in this Parquet file
 */
class ParquetPartition(val locator: FileLocator,
                       val index: Int,
                       val rowGroup: ParquetRowGroup,
                       val requestedSchema: ParquetSchemaType,
                       val actualSchema: ParquetSchemaType)
    extends Partition {

  def materializeRecords[T](io: ByteAccess,
                            recordMaterializer: RecordMaterializer[T],
                            filter: UnboundRecordFilter): Iterator[T] =
    ParquetPartition.materializeRecords(io, recordMaterializer, filter, rowGroup, requestedSchema, actualSchema)

}

/**
 * PartitionPageReadStore is a holder for mapping between the ParquetColumnDescriptor values and the
 * PageReader values.
 *
 * @param chunkMap For a particular row group in a Parquet file, this is a map relating the descriptors of the columns
 *                 to the corresponding readers for their values.
 * @param rowGroup The metadata for the row group itself.
 */
class PartitionPageReadStore(chunkMap: Map[ParquetColumnDescriptor, PageReader], rowGroup: ParquetRowGroup)
    extends PageReadStore {

  override def getPageReader(cd: ColumnDescriptor): PageReader =
    chunkMap.getOrElse(new ParquetColumnDescriptor(cd),
      throw new NoSuchElementException("Could not find %s in the map %s".format(
        cd.getPath.mkString("."),
        chunkMap.keys.map(_.path.mkString(".")).mkString(","))))
  override def getRowCount: Long = rowGroup.rowCount
}

object ParquetPartition {

  /**
   * This is our core implementation that materializes records out of a row group,
   * applying the corresponding filters and projections.
   *
   * @param io The bytes (reified as a ByteAccess value) for the Parquet file
   * @param recordMaterializer a record materializer for the records we are requesting back
   * @param filter The filter to dictate which records should be materialized.
   * @param rowGroup The metadata for the row group we are materializing records from
   * @param requestedSchema The projection requested by the user
   * @param actualSchema The schema of the records in the file
   * @tparam T The type of the records desired by the user
   * @return An iterator which materializes only those records passing the filter from the row group
   */
  def materializeRecords[T](io: ByteAccess,
                            recordMaterializer: RecordMaterializer[T],
                            filter: UnboundRecordFilter,
                            rowGroup: ParquetRowGroup,
                            requestedSchema: ParquetSchemaType,
                            actualSchema: ParquetSchemaType): Iterator[T] = {

    val requestedPaths = requestedSchema.paths()

    val requestedColumnChunks: Seq[ParquetColumnChunk] = rowGroup.columnChunks.filter {
      cc => requestedPaths.contains(TypePath(cc.columnDescriptor.path))
    }

    val config: Configuration = new Configuration()

    val decompressor: Option[HadoopCompressionCodec] =
      CompressionCodecEnum.getHadoopCodec(rowGroup.columnChunks.head.compressionCodec, config)

    val chunkMap = requestedColumnChunks
      .map(cc => (cc.columnDescriptor, cc.readAllPages(decompressor, io)))
      .toMap
    val pageReadStore = new PartitionPageReadStore(chunkMap, rowGroup)

    val columnIOFactory: ColumnIOFactory = new ColumnIOFactory
    val columnIO = columnIOFactory.getColumnIO(requestedSchema.convertToParquet(), actualSchema.convertToParquet())
    val reader = columnIO.getRecordReader[T](pageReadStore, recordMaterializer, filter)

    new Iterator[T] {
      var recordsRead = 0
      val totalRecords = rowGroup.rowCount
      var nextT: Option[T] = Option(reader.read())

      override def next(): T = {
        val ret = nextT
        recordsRead += 1
        if (recordsRead >= totalRecords) {
          nextT = None
        } else {
          nextT = Option(reader.read())
        }
        ret.getOrElse(null.asInstanceOf[T])
      }

      override def hasNext: Boolean = nextT.isDefined
    }

  }
}
