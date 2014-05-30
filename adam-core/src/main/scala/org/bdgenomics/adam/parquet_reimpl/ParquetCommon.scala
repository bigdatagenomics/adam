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

import java.io.{ ByteArrayInputStream, InputStream }
import java.nio.charset.Charset
import org.bdgenomics.adam.rdd.CompressionCodecEnum.CompressionCodec
import parquet.column.page.PageReader
import parquet.format.{ ColumnChunk, RowGroup, FileMetaData }
import parquet.format.converter.ParquetMetadataConverter
import parquet.org.apache.thrift.protocol.TCompactProtocol
import parquet.org.apache.thrift.transport.TIOStreamTransport
import parquet.schema.MessageType
import parquet.schema.PrimitiveType.PrimitiveTypeName
import scala.collection.JavaConversions._
import scala.math._

package parquet.format.converter {

  /**
   * Parquet has made their particular methods for doing this conversion
   * private, protected, and package-protected, so we have to wrap it
   * _within the parquet.format.converter package_.
   */
  class MyConverter extends ParquetMetadataConverter with Serializable {
    def convert(md: FileMetaData): MessageType = super.fromParquetSchema(md.getSchema)
  }
}

package org.bdgenomics.adam.rdd {

  import parquet.format.converter.MyConverter
  import java.nio.ByteBuffer
  import parquet.column.page.{ DictionaryPage, Page }
  import parquet.format.{ PageType, PageHeader }
  import parquet.bytes.BytesInput
  import parquet.column.ColumnDescriptor
  import org.apache.hadoop.io.compress.{ CompressionCodec => HadoopCompressionCodec, CodecPool }
  import org.apache.hadoop.conf.Configuration
  import org.bdgenomics.adam.io.ByteAccess

  object ParquetCommon {

    val MAGIC: Array[Byte] = "PAR1".getBytes(Charset.forName("ASCII"))

    val converter = new MyConverter()

    def parseMessageType(metadata: FileMetaData): MessageType = converter.convert(metadata)

    def readInt(buffer: Array[Byte], offset: Int): Int =
      ByteBuffer.wrap(buffer.slice(offset, offset + 4).reverse).getInt

    def readFileMetadata(io: ByteAccess): FileMetaData = {
      val magicAndLength: Array[Byte] = io.readFully(io.length() - MAGIC.length - 4, MAGIC.length + 4)

      require(magicAndLength.slice(4, magicAndLength.length).sameElements(MAGIC),
        "File does not appear to be a Parquet file")

      val footerLength = readInt(magicAndLength, 0)

      assert(io.length() >= 0)
      assert(footerLength >= 0, "footerLength %d should be non-negative".format(footerLength))
      assert(io.length() - MAGIC.length - 4 - footerLength >= 0)

      val footerBytes = io.readFully(io.length() - MAGIC.length - 4 - footerLength, footerLength)
      val footerInputStream = new ByteArrayInputStream(footerBytes)

      val protocol = new TCompactProtocol(new TIOStreamTransport(footerInputStream))
      val fileMetadata = new FileMetaData()
      fileMetadata.read(protocol)

      fileMetadata
    }

    def readFooter(io: ByteAccess): Footer = {
      new Footer(readFileMetadata(io))
    }
  }

  /**
   *
   * @param byteSize Total (compressed) size of the row group, in bytes.
   * @param rowCount Total number of rows in the row group
   * @param columnChunks Metadata for each column chunk in the row group
   */
  case class ParquetRowGroup(byteSize: Long, rowCount: Long, columnChunks: Seq[ParquetColumnChunk]) {
    def this(rg: RowGroup, schema: MessageType) = this(rg.getTotal_byte_size, rg.getNum_rows, rg.getColumns.map(cc => new ParquetColumnChunk(cc, schema)))

  }

  object CompressionCodecEnum extends Enumeration {
    val GZIP, SNAPPY, LZO, UNCOMPRESSED = Value

    type CompressionCodec = CompressionCodecEnum.Value

    def apply(codec: parquet.format.CompressionCodec): CompressionCodec =
      codec match {
        case parquet.format.CompressionCodec.GZIP         => GZIP
        case parquet.format.CompressionCodec.SNAPPY       => SNAPPY
        case parquet.format.CompressionCodec.LZO          => LZO
        case parquet.format.CompressionCodec.UNCOMPRESSED => UNCOMPRESSED
      }

    def getHadoopCodec(parquetCodec: CompressionCodec, conf: Configuration): Option[HadoopCompressionCodec] = {
      val codec = parquetCodec match {
        case GZIP   => Some(new org.apache.hadoop.io.compress.GzipCodec())
        case SNAPPY => Some(new parquet.hadoop.codec.SnappyCodec())
        case LZO =>
          throw new UnsupportedOperationException("Cannot initialize an LZO Codec.")
        case UNCOMPRESSED => None
      }

      codec.foreach {
        cd =>
          cd.setConf(conf)
      }

      codec
    }

  }

  case class ParquetColumnDescriptor(path: Seq[String],
                                     typeName: PrimitiveTypeName,
                                     typeLength: Int,
                                     maxRep: Int,
                                     maxDef: Int) {

    def this(descriptor: ColumnDescriptor) =
      this(descriptor.getPath, descriptor.getType, descriptor.getTypeLength, descriptor.getMaxRepetitionLevel, descriptor.getMaxRepetitionLevel)

    def this(cc: ColumnChunk, schema: MessageType) =
      this(schema
        .getColumns
        .find(_.getPath.sameElements(cc.getMeta_data.getPath_in_schema))
        .get)

    def columnDescriptor(): ColumnDescriptor =
      new ColumnDescriptor(path.toArray, typeName, typeLength, maxRep, maxDef)

    override def toString: String = "{path=%s|typeName=%s|typeLength=%d|maxRep=%d|maxDef=%d}".format(
      path.mkString("."), typeName, typeLength, maxRep, maxDef)
  }

  /**
   *
   * @param numValues The number of values in this column
   * @param dataPageOffset The offset relative to the entire file, in bytes, of the first data page within this column chunk
   * @param indexPageOffset The offset, relative to the entire file, in bytes, of the (optional) index page
   * @param dictionaryPageOffset The offset, relative to the entire file, in bytes, of the (optional) dictionary page
   * @param uncompressedSize The total size, in uncompressed bytes, of the column chunk
   * @param compressedSize The total compressed size, in bytes, of the column chunk
   * @param columnDescriptor The column descriptor for this column chunk, describes path and type information
   * @param compressionCodec The compression method used on this column chunk
   */
  case class ParquetColumnChunk(numValues: Long,
                                dataPageOffset: Long,
                                indexPageOffset: Option[Long],
                                dictionaryPageOffset: Option[Long],
                                uncompressedSize: Long,
                                compressedSize: Long,
                                columnDescriptor: ParquetColumnDescriptor,
                                compressionCodec: CompressionCodec) {

    def this(cc: ColumnChunk, schema: MessageType) =
      this(
        cc.getMeta_data.getNum_values,
        cc.getMeta_data.getData_page_offset,
        if (cc.getMeta_data.isSetIndex_page_offset) Some(cc.getMeta_data.getIndex_page_offset) else None,
        if (cc.getMeta_data.isSetDictionary_page_offset) Some(cc.getMeta_data.getDictionary_page_offset) else None,
        cc.getMeta_data.getTotal_uncompressed_size,
        cc.getMeta_data.getTotal_compressed_size,
        new ParquetColumnDescriptor(cc, schema),
        CompressionCodecEnum(cc.getMeta_data.getCodec))

    def readPageHeader(is: InputStream): PageHeader = parquet.format.Util.readPageHeader(is)

    def readDataPage(bytesInput: BytesInput, pageHeader: PageHeader, parquetMetadataConverter: ParquetMetadataConverter): Page =
      new Page(
        bytesInput,
        pageHeader.data_page_header.num_values,
        pageHeader.uncompressed_page_size,
        parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
        parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
        parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding))

    def readDictionaryPage(bytesInput: BytesInput, pageHeader: PageHeader, parquetMetadataConverter: ParquetMetadataConverter): DictionaryPage =
      new DictionaryPage(
        bytesInput,
        pageHeader.uncompressed_page_size,
        pageHeader.dictionary_page_header.num_values,
        parquetMetadataConverter.getEncoding(pageHeader.dictionary_page_header.encoding))

    private def decompress(codec: HadoopCompressionCodec, input: InputStream, compressed: Int, uncompressed: Int): BytesInput = {
      val decompressor = CodecPool.getDecompressor(codec)
      decompressor.reset()
      val bytesArray = new Array[Byte](compressed)
      var toRead = compressed
      var read = 1
      while (read > 0 && toRead > 0) {

        assert(compressed - toRead >= 0, "offset %d-%d=%d was negative".format(compressed, toRead, compressed - toRead))
        assert(toRead >= 0, "len %d was negative".format(toRead))
        assert(toRead <= bytesArray.length - (compressed - toRead), "length was > the bounds %d".format(toRead))
        read = input.read(bytesArray, compressed - toRead, toRead)
        toRead = toRead - read
      }
      val bytes = new ByteArrayInputStream(bytesArray)

      assert(codec != null, "codec was null")
      assert(decompressor != null, "decompressor was null")
      assert(bytes != null, "bytes was null")
      BytesInput.from(codec.createInputStream(bytes, decompressor), uncompressed)
    }

    def readAllPages(codec: Option[HadoopCompressionCodec], io: ByteAccess): PageReader = {

      val parquetMetadataConverter = ParquetCommon.converter

      val chunkBytes: Array[Byte] =
        io.readFully(min(min(dictionaryPageOffset.getOrElse(Long.MaxValue), indexPageOffset.getOrElse(Long.MaxValue)),
          dataPageOffset),
          compressedSize.toInt)
      val input = new ByteArrayInputStream(chunkBytes)

      var dictPage: Option[DictionaryPage] = None
      var dataPages: Seq[Page] = Seq()

      var valuesRead: Long = 0

      while (valuesRead < numValues) {
        val header = readPageHeader(input)
        val bytesInput = codec match {
          case None => BytesInput.from(input, header.compressed_page_size)
          case Some(c) =>
            decompress(c, input, header.compressed_page_size, header.uncompressed_page_size)
        }

        header.`type` match {
          case PageType.DICTIONARY_PAGE =>
            dictPage = Some(readDictionaryPage(bytesInput, header, parquetMetadataConverter))
          case PageType.DATA_PAGE =>
            val dp = readDataPage(bytesInput, header, parquetMetadataConverter)
            dataPages = dataPages :+ dp
            valuesRead += dp.getValueCount
        }
      }

      if (valuesRead != numValues) {
        // corrupt chunk
      }

      new PageReader {
        override def readPage: Page = {
          if (dataPages.isEmpty) null
          else {
            val ret = dataPages.head
            dataPages = dataPages.tail
            ret
          }
        }

        override def readDictionaryPage: DictionaryPage = {
          dictPage match {
            case None     => null
            case Some(dp) => dp
          }
        }

        def getTotalValueCount: Long = numValues
      }
    }

  }

  /**
   *
   * @param rowGroups The metadata for each row group in the file
   */
  case class Footer(rowGroups: Seq[ParquetRowGroup]) {

    def this(metadata: FileMetaData) = this(Footer.parseRowGroups(metadata))

  }

  object Footer {
    def parseRowGroups(metadata: FileMetaData): Seq[ParquetRowGroup] = {
      val schema = ParquetCommon.parseMessageType(metadata)
      metadata.getRow_groups.map(rg => new ParquetRowGroup(rg, schema))
    }
  }

}
