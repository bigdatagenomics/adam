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
package org.bdgenomics.adam.serialization

import java.io.{ IOException, InputStream }
import java.nio.ByteBuffer
import java.util
import java.util.Map.Entry

import org.apache.avro.io.{ BinaryDecoder, Decoder, DecoderFactory }
import org.apache.avro.util.Utf8

/**
 * Note: A LinkedHashMap takes 40 * SIZE + 4 * CAPACITY bytes. If the size == capacity, then the memory
 * footprint is CAPACITY * 44 bytes.
 */
private[serialization] class LRUCache[K, V](maxCapacity: Int, loadFactor: Double = 0.75f)
    extends util.LinkedHashMap[K, V](maxCapacity, 0.75f, true) {
  assert(maxCapacity > 0, "LRU cache maxCapacity should be greater than 0")
  assert(0 < loadFactor && loadFactor <= 1.0, "LRU load factor must be (0,1.0]")
  override def removeEldestEntry(eldest: Entry[K, V]): Boolean = {
    size() > maxCapacity
  }
}

/**
 * An Avro binary decoder that keep a LRU cache of strings to minimize memory pressure.
 *
 * WARNING: This class is not thread-safe! This isn't an issue since Spark guarantees that only
 * a single thread will access it.
 *
 * A amount of memory strings take in Java is 8 * (int) ((((#chars) * 2) + 45) / 8) bytes, e.g. a string that is
 * 128 characters long will take, 8 * ((((128) * 2) + 45) / 8) = 296 bytes. This BinaryDecoder is not for general
 * use but is optimized for ADAM -omics data, where strings are typically ~128 characters long.
 *
 * The default cache capacity size of 64K elements, equates to a maximum memory use of (44*64K) for the cache and
 * (296*64K) for the strings, a total of ~22 MB when full.
 *
 * @param in The input stream of Avro data
 */
private[serialization] class BinaryDecoderWithLRUCache(
    in: InputStream,
    maxCacheableStringSize: Int,
    cache: LRUCache[Utf8, Utf8]) extends Decoder {

  def this(in: InputStream, maxCacheableStringSize: Int = 50, cacheCapacity: Int = 65535) =
    this(in, maxCacheableStringSize, new LRUCache[Utf8, Utf8](cacheCapacity))

  val wrappedDecoder = DecoderFactory.get().directBinaryDecoder(in, null.asInstanceOf[BinaryDecoder])

  private val scratchUtf8: Utf8 = new Utf8

  @throws(classOf[IOException])
  override def readString: String = {
    readString(scratchUtf8).toString
  }

  @throws(classOf[IOException])
  override def readString(old: Utf8): Utf8 = {
    // NOTE: The 'old' parameter is intentionally being ignored. We always reuse 'scratchUtf8' to keep
    // from creating a new Utf8 object every time we read a string. This isn't thread-safe, of course,
    // but Spark ensures that each thread has its own serialization infrastructure.
    val readValue = wrappedDecoder.readString(scratchUtf8)
    if (readValue.length() > maxCacheableStringSize) {
      if (old == null) {
        new Utf8(readValue)
      } else {
        old.set(Option(readValue).map(_.toString).orNull)
      }
    } else {
      if (!cache.containsKey(readValue)) {
        val valueCopy = new Utf8(readValue)
        cache.put(valueCopy, valueCopy)
      }
      cache.get(readValue)
    }
  }

  override def readLong(): Long = wrappedDecoder.readLong()

  override def readFloat(): Float = wrappedDecoder.readFloat()

  override def readNull(): Unit = wrappedDecoder.readNull()

  override def skipArray(): Long = wrappedDecoder.skipArray()

  override def skipMap(): Long = wrappedDecoder.skipMap()

  override def arrayNext(): Long = wrappedDecoder.arrayNext()

  override def mapNext(): Long = wrappedDecoder.mapNext()

  override def readFixed(bytes: Array[Byte], start: Int, length: Int): Unit =
    wrappedDecoder.readFixed(bytes, start, length)

  override def readEnum(): Int = wrappedDecoder.readEnum()

  override def readMapStart(): Long = wrappedDecoder.readMapStart()

  override def readInt(): Int = wrappedDecoder.readInt()

  override def readBytes(old: ByteBuffer): ByteBuffer = wrappedDecoder.readBytes(old)

  override def skipFixed(length: Int): Unit = wrappedDecoder.skipFixed(length)

  override def readArrayStart(): Long = wrappedDecoder.readArrayStart()

  override def skipBytes(): Unit = wrappedDecoder.skipBytes()

  override def readBoolean(): Boolean = wrappedDecoder.readBoolean()

  override def readIndex(): Int = wrappedDecoder.readIndex()

  override def skipString(): Unit = wrappedDecoder.skipString()

  override def readDouble(): Double = wrappedDecoder.readDouble()
}
