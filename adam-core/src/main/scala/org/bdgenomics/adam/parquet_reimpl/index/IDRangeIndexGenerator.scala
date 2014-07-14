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
package org.bdgenomics.adam.parquet_reimpl.index

import java.io._
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.spark.Logging
import org.bdgenomics.adam.io.{ ByteAccess, FileLocator, LocalFileLocator }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.parquet_reimpl._
import org.bdgenomics.adam.rdd._
import parquet.avro.{ AvroSchemaConverter, UsableAvroRecordMaterializer }
import parquet.filter.UnboundRecordFilter
import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType
import scala.collection.mutable
import scala.reflect._

class IDRangeIndexGenerator[T <: IndexedRecord](idAccessor: T => String,
                                                indexableSchema: Option[Schema] = None)(implicit referenceFolder: ReferenceFolder[T],
                                                                                        classTag: ClassTag[T])
    extends Logging {

  val avroSchema: Schema = classTag.runtimeClass.newInstance().asInstanceOf[T].getSchema
  val filter: UnboundRecordFilter = null

  /**
   * TODO: Need to replace this (and identical implementatoins in other classes) with a
   * single implementation.
   */
  def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
    schema match {
      case None    => fileMessageType
      case Some(s) => new AvroSchemaConverter().convert(s)
    }

  class IDAndRangeSet {

    val idSet: mutable.HashSet[String] = mutable.HashSet()
    var rangeSet: Seq[ReferenceRegion] = Seq()
  }

  def folder(folded: IDAndRangeSet, value: T): IDAndRangeSet = {
    folded.rangeSet = referenceFolder.fold(folded.rangeSet, value)
    folded.idSet.add(idAccessor(value))

    folded
  }

  def idRanges(rowGroup: ParquetRowGroup,
               io: ByteAccess,
               materializer: RecordMaterializer[T],
               reqSchema: ParquetSchemaType,
               actualSchema: ParquetSchemaType): (Seq[String], Seq[ReferenceRegion]) = {

    val idAndRangeSet =
      ParquetPartition.materializeRecords(io, materializer, filter, rowGroup, reqSchema, actualSchema).
        foldLeft(new IDAndRangeSet())(folder)

    (idAndRangeSet.idSet.toSeq, idAndRangeSet.rangeSet)
  }

  def addParquetFile(fullPath: String): Iterator[IDRangeIndexEntry] = {
    val file = new File(fullPath)
    val rootLocator = new LocalFileLocator(file.getParentFile)
    val relativePath = file.getName
    if (file.isFile) {
      logInfo("Indexing file %s, relative path %s".format(fullPath, relativePath))
      addParquetFile(rootLocator, relativePath)
    } else {
      val childFiles = file.listFiles().filter(f => f.isFile && !f.getName.startsWith("."))
      childFiles.flatMap {
        case f =>
          val childRelativePath = "%s/%s".format(relativePath, f.getName)
          logInfo("Indexing child file %s, relative path %s".format(f.getName, childRelativePath))
          addParquetFile(rootLocator, childRelativePath)
      }.iterator
    }
  }

  def addParquetFile(rootLocator: FileLocator, relativePath: String): Iterator[IDRangeIndexEntry] = {
    val locator = rootLocator.relativeLocator(relativePath)
    val io = locator.bytes
    val footer: Footer = ParquetCommon.readFooter(io)
    val fileMessageType: MessageType = ParquetCommon.parseMessageType(ParquetCommon.readFileMetadata(io))
    val actualSchema: ParquetSchemaType = new ParquetSchemaType(fileMessageType)
    val requestedMessageType: MessageType = convertAvroSchema(indexableSchema, fileMessageType)
    val reqSchema: ParquetSchemaType = new ParquetSchemaType(requestedMessageType)
    val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, avroSchema)

    logInfo("# row groups: %d".format(footer.rowGroups.length))
    logInfo("# total records: %d".format(footer.rowGroups.map(_.rowCount).sum))

    footer.rowGroups.zipWithIndex.flatMap {
      case (rowGroup: ParquetRowGroup, i: Int) =>
        logInfo("row group %d, # records %d".format(i, rowGroup.rowCount))
        val (idSet, rangeSet) = idRanges(rowGroup, io, avroRecordMaterializer, reqSchema, actualSchema)

        idSet.flatMap {
          case id =>
            rangeSet.map(range => new IDRangeIndexEntry(relativePath, i, id, range))
        }
    }.iterator
  }
}

