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

import java.io._
import org.apache.avro.generic.IndexedRecord
import org.apache.spark.Logging
import org.apache.avro.Schema
import org.bdgenomics.adam.rdd._
import org.bdgenomics.adam.io.{ FileLocator, LocalFileLocator, ByteAccess }
import parquet.avro.{ AvroSchemaConverter, UsableAvroRecordMaterializer }
import parquet.schema.MessageType
import parquet.filter.UnboundRecordFilter
import parquet.io.api.RecordMaterializer
import scala.reflect._

/**
 * ParquetLister materializes the records within a Parquet row group as an Iterator[T].
 *
 * This is used for two purposes at the moment:
 * 1. we've added a 'PrintParquet' command (which works only for ADAMFlatGenotype parquet
 *    files, at the moment), whose purpose is to print out a number of entries from within
 *    a Parquet file for debugging purposes.  PrintParquet uses ParquetLister to get those
 *    entries and materialize them.
 * 2. The index generators (for the Range and IDRange indices) need to read through a
 *    Parquet files entries in order to index it -- if you're not building the index at
 *    the time you write the file, that is. So they have to materialize the records in the
 *    file, and they use the ParquetLister to do that.
 *
 * Materializing records from a Parquet file turns out to be a somewhat-complicated operation,
 * with a few parameters floating around. The original version was written as part of the
 * materialize methods in the two Parquet RDDs, but we factored it out into this class
 * when we realized we needed it for the two additional purposes (listed above).
 *
 * ParquetLister doesn't yet support UnboundRecordFilter (see the comment below) but it should
 * be adapted to do so.  When it does, we should replace the original materialize methods
 * in the ParquetRDDs with use of this class instead, so that there's only one implementation
 * of the materialize code floating around.
 *
 * @param indexableSchema An (optional) Schema indicating which fields to read from the Parquet
 *                        file.
 * @param classTag The type of the record (T) must be manifest.
 * @tparam T The type of the record to be read.
 */
class ParquetLister[T <: IndexedRecord](indexableSchema: Option[Schema] = None)(implicit classTag: ClassTag[T])
    extends Logging {

  val avroSchema: Schema = classTag.runtimeClass.newInstance().asInstanceOf[T].getSchema

  // this is 'null' by default, but (if we allowed it to be a parameter to the class),
  // we could use pushdown-predicates as part of the lister.
  val filter: UnboundRecordFilter = null

  private def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
    schema match {
      case None    => fileMessageType
      case Some(s) => new AvroSchemaConverter().convert(s)
    }

  private def materialize(rowGroup: ParquetRowGroup,
                          io: ByteAccess,
                          materializer: RecordMaterializer[T],
                          reqSchema: ParquetSchemaType,
                          actualSchema: ParquetSchemaType): Iterator[T] =
    ParquetPartition.materializeRecords(io, materializer, filter, rowGroup, reqSchema, actualSchema)

  /**
   * Given a full path to the local filesystem, checks whether the path is a file --
   * in which case, it materializes the row groups from that file -- or if it's a directory.
   * If the path is a directory, it lists the row groups of all the files within the directory.
   *
   * @param fullPath The path on the local filesystem, corresponding either to a Parquet file
   *                 or a directory filled with parquet files.
   * @return An iterator over the values requested from the file (or, in case fullPath is a directory,
   *         _all_ the files in some arbitrary order)
   */
  def materialize(fullPath: String): Iterator[T] = {
    val file = new File(fullPath)
    val rootLocator = new LocalFileLocator(file.getParentFile)
    val relativePath = file.getName
    if (file.isFile) {
      logInfo("Indexing file %s, relative path %s".format(fullPath, relativePath))
      materialize(rootLocator, relativePath)
    } else {
      val childFiles = file.listFiles().filter(f => f.isFile && !f.getName.startsWith("."))
      childFiles.flatMap {
        case f =>
          val childRelativePath = "%s/%s".format(relativePath, f.getName)
          try {
            logInfo("Indexing child file %s, relative path %s".format(f.getName, childRelativePath))
            materialize(rootLocator, childRelativePath)
          } catch {
            case e: IllegalArgumentException =>
              logInfo("File %s/%s doesn't appear to be a Parquet file; skipping".format(f.getName, childRelativePath))
              Seq()
          }
      }.iterator
    }
  }

  /**
   * Materializes _all_ the row groups for a particular Parquet file, given as a
   * root locator (the parent directory, for instance) and a relative path (the location
   * of the file).
   *
   * The particular form of this method and its arguments it driven by two concerns:
   * 1. recursive listing of Parquet files in the local filesystem, see the
   *    materialize(fullPath : String) method above.
   * 2. when reading files from S3, we often store the locations of our files in terms of a
   *    root path (bucket + root key) and relative offsets (which are the names read
   *    from the index itself).
   *
   * @param rootLocator A base locator, relative to which the relativePath will point to a valid parquet file
   * @param relativePath A relative path to a Parquet file
   * @return An iterator over T values from the named parquet file
   */
  def materialize(rootLocator: FileLocator, relativePath: String): Iterator[T] = {
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
        materialize(rowGroup, io, avroRecordMaterializer, reqSchema, actualSchema)
    }.iterator
  }
}

