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

package org.bdgenomics.adam.util

import java.nio.{ ByteBuffer, ByteOrder }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.bdgenomics.utils.io.{ ByteAccess, ByteArrayByteAccess }
import org.bdgenomics.adam.models._

private object TwoBitFile {
  val MAGIC_NUMBER: Int = 0x1A412743
  val BASES_PER_BYTE: Int = 4
  val BYTE_SIZE: Int = 8
  val MASK: Byte = 3 // 00000011

  // file-level byte offsets
  val VERSION_OFFSET: Int = 4
  val SEQUENCE_COUNT_OFFSET: Int = 8
  val HEADER_RESERVED_OFFSET: Int = 12
  val FILE_INDEX_OFFSET: Int = 16

  // index record-related sizes (bytes)
  val NAME_SIZE_SIZE: Int = 1
  val OFFSET_SIZE: Int = 4

  // sequence record-related sizes (bytes)
  val INT_SIZE: Int = 4
  val DNA_SIZE_SIZE: Int = INT_SIZE
  val BLOCK_COUNT_SIZE: Int = 4
  // 4-byte int for Starts array and 4-byte int for Sizes array
  val PER_BLOCK_SIZE: Int = 8
  val SEQ_RECORD_RESERVED_SIZE: Int = 4
}

/**
 * Represents a set of reference sequences backed by a .2bit file.
 *
 * See http://genome.ucsc.edu/FAQ/FAQformat.html#format7 for the spec.
 *
 * @param byteAccess ByteAccess pointing to a .2bit file.
 */
class TwoBitFile(byteAccess: ByteAccess) extends ReferenceFile {

  // load file into memory
  private[util] val bytes = ByteBuffer.wrap(byteAccess.readFully(0, byteAccess.length().toInt))
  private[util] val numSeq = readHeader()
  // hold current byte position of start of current index record
  var indexRecordStart = TwoBitFile.FILE_INDEX_OFFSET
  private val seqRecordStarts = (0 until numSeq).map(i => {
    val tup = readIndexEntry(indexRecordStart)
    indexRecordStart += TwoBitFile.NAME_SIZE_SIZE + tup._1.length + TwoBitFile.OFFSET_SIZE
    tup
  }).toMap
  private[util] val seqRecords = seqRecordStarts.map(tup => tup._1 -> TwoBitRecord(bytes, tup._1, tup._2))

  /**
   * The sequence dictionary corresponding to the contigs in this two bit file.
   */
  val sequences = new SequenceDictionary(seqRecords.toVector.map(r => SequenceRecord(r._1, r._2.dnaSize)))

  private def readHeader(): Int = {
    // figure out proper byte order
    bytes.order(ByteOrder.LITTLE_ENDIAN)
    if (bytes.getInt(0) != TwoBitFile.MAGIC_NUMBER) {
      bytes.order(ByteOrder.BIG_ENDIAN)
    }
    if (bytes.getInt(0) != TwoBitFile.MAGIC_NUMBER) {
      throw new IllegalStateException()
    }
    // process header
    assert(bytes.getInt(TwoBitFile.VERSION_OFFSET) == 0, "Version must be zero")
    assert(bytes.getInt(TwoBitFile.HEADER_RESERVED_OFFSET) == 0, "Reserved field must be zero")
    assert(bytes.hasArray)
    bytes.getInt(TwoBitFile.SEQUENCE_COUNT_OFFSET)
  }

  private def readIndexEntry(indexRecordStart: Int): (String, Int) = {
    val nameSize = bytes.get(indexRecordStart).toInt
    val name = new String(bytes.array, indexRecordStart + TwoBitFile.NAME_SIZE_SIZE, nameSize, "UTF-8")
    val contigOffset = bytes.getInt(indexRecordStart + TwoBitFile.NAME_SIZE_SIZE + nameSize)
    name -> contigOffset
  }

  /**
   * Extract reference sequence from the .2bit data.
   *
   * @param region The desired ReferenceRegion to extract.
   * @param mask Whether to apply masks (with lowercase letters)
   * @return The reference sequence at the desired locus.
   */
  def extract(region: ReferenceRegion, mask: Boolean): String = {
    val record =
      seqRecords.getOrElse(
        region.referenceName,
        throw new Exception(
          s"Contig ${region.referenceName} not found in reference map with keys: ${seqRecords.keys.toList.sortBy(x => x).mkString(", ")}"
        )
      )
    val contigLength = record.dnaSize
    assert(region.start >= 0)
    assert(region.end <= contigLength.toLong)
    val offset = record.dnaOffset
    val sb = StringBuilder.newBuilder

    val nBlocks: Array[Long] = if (record.nBlocks.isEmpty) {
      Array(-1L)
    } else {
      record.nBlocks.get.endpoints ++ Array(-1L)
    }
    val maskBlocks: Array[Long] = if (record.maskBlocks.isEmpty) {
      Array(-1L)
    } else {
      record.maskBlocks.get.endpoints ++ Array(-1L)
    }

    var currentNBlock = 0
    var currentMaskBlock = 0
    while (nBlocks(currentNBlock) != -1 && region.start.toInt >= nBlocks(currentNBlock + 1)) {
      currentNBlock += 2
    }
    while (maskBlocks(currentMaskBlock) != -1 && region.start.toInt >= maskBlocks(currentMaskBlock + 1)) {
      currentMaskBlock += 2
    }

    for (i <- 0 until region.width.toInt) {
      // we step into an N block
      val nt = if (nBlocks(currentNBlock) != -1 && region.start.toInt + i >= nBlocks(currentNBlock)) {
        if (region.start.toInt + i + 1 == nBlocks(currentNBlock + 1)) {
          currentNBlock += 2
        }
        'N'
      } else {
        // TODO: this redundantly reads the byte at a given offset
        // pull out the byte that contains the current base
        val byte = bytes.get(offset + (region.start.toInt + i) / TwoBitFile.BASES_PER_BYTE)
        // which slot in the byte does our base occupy?
        // 1: 11000000; 2: 00110000; 3: 00001100; 4: 00000011
        val slot = (region.start + i) % TwoBitFile.BASES_PER_BYTE + 1
        val shift = TwoBitFile.BYTE_SIZE - 2 * slot
        // move the 2-bit base to the least significant position
        // and zero out the more significant bits
        (byte >> shift) & TwoBitFile.MASK match {
          case 0 => 'T'
          case 1 => 'C'
          case 2 => 'A'
          case 3 => 'G'
        }
      }
      // if nt is masked then make it lower case
      val maskedNt = if (mask && maskBlocks(currentMaskBlock) != -1 && region.start.toInt + i >= maskBlocks(currentMaskBlock)) {
        if (region.start.toInt + i + 1 == maskBlocks(currentMaskBlock + 1)) {
          currentMaskBlock += 2
        }
        nt.toLower
      } else {
        nt
      }
      sb += maskedNt
    }
    sb.toString()
  }

  /**
   * Extract reference sequence from the .2bit data.  Do not apply masks.
   *
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  def extract(region: ReferenceRegion): String = {
    extract(region, false)
  }
}

class TwoBitFileSerializer extends Serializer[TwoBitFile] {
  override def write(kryo: Kryo, output: Output, obj: TwoBitFile): Unit = {
    val arr = obj.bytes.array()
    output.writeInt(arr.length)
    output.write(arr)
  }

  override def read(kryo: Kryo, input: Input, klazz: Class[TwoBitFile]): TwoBitFile = {
    val length = input.readInt()
    val bytes = input.readBytes(length)
    new TwoBitFile(new ByteArrayByteAccess(bytes))
  }
}

private object TwoBitRecord {
  def apply(twoBitBytes: ByteBuffer, name: String, seqRecordStart: Int): TwoBitRecord = {
    val dnaSize = twoBitBytes.getInt(seqRecordStart)
    val nBlockCount = twoBitBytes.getInt(seqRecordStart + TwoBitFile.DNA_SIZE_SIZE)
    val nBlockArraysOffset = seqRecordStart + TwoBitFile.DNA_SIZE_SIZE + TwoBitFile.BLOCK_COUNT_SIZE
    val nBlocks = if (nBlockCount == 0) None else Some(NonoverlappingRegions((0 until nBlockCount).map(i => {
      // reading into an array of ints
      val nBlockStart = twoBitBytes.getInt(nBlockArraysOffset + i * TwoBitFile.INT_SIZE)
      val nBlockSize = twoBitBytes.getInt(nBlockArraysOffset + (nBlockCount * TwoBitFile.INT_SIZE) + i * TwoBitFile.INT_SIZE)
      ReferenceRegion(name, nBlockStart, nBlockStart + nBlockSize) -> None
    })))
    val maskBlockCount = twoBitBytes.getInt(nBlockArraysOffset + (nBlockCount * TwoBitFile.PER_BLOCK_SIZE))
    val maskBlockArraysOffset = nBlockArraysOffset + (nBlockCount * TwoBitFile.PER_BLOCK_SIZE) + TwoBitFile.BLOCK_COUNT_SIZE
    val maskBlocks = if (maskBlockCount == 0) None else Some(NonoverlappingRegions((0 until maskBlockCount).map(i => {
      // reading into an array of ints
      val maskBlockStart = twoBitBytes.getInt(maskBlockArraysOffset + i * TwoBitFile.INT_SIZE)
      val maskBlockSize = twoBitBytes.getInt(maskBlockArraysOffset + (maskBlockCount * TwoBitFile.INT_SIZE) + i * TwoBitFile.INT_SIZE)
      ReferenceRegion(name, maskBlockStart, maskBlockStart + maskBlockSize) -> None
    })))
    val dnaOffset = maskBlockArraysOffset + (maskBlockCount * TwoBitFile.PER_BLOCK_SIZE) + TwoBitFile.SEQ_RECORD_RESERVED_SIZE
    TwoBitRecord(dnaSize, nBlocks, maskBlocks, dnaOffset)
  }
}

private case class TwoBitRecord(dnaSize: Int,
                                nBlocks: Option[NonoverlappingRegions],
                                maskBlocks: Option[NonoverlappingRegions],
                                dnaOffset: Int) {
}
