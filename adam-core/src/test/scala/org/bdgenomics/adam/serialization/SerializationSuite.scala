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

import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.avro.io.{ BinaryEncoder, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }
import org.apache.avro.util.Utf8
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class SerializationSuite extends ADAMFunSuite {

  test("LRU cache maxCapacity is enforced and LRU is evicted") {
    val capacity = 10
    val cache = new LRUCache[Int, Int](capacity)
    0 to capacity * 2 map (i => cache.put(i, i))
    assert(cache.size() == capacity)
    0 to capacity foreach (i => assert(!cache.containsKey(i)))
    capacity + 1 to capacity * 2 foreach (i => assert(cache.containsKey(i)))
  }

  test("BinaryDecoderWithLRUCache evicts LRU strings") {
    val maxCacheableStringLength = 8
    val cacheCapacity = 10
    val cache = new LRUCache[Utf8, Utf8](cacheCapacity)
    val outstream = new FastByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])
    val writer = new SpecificDatumWriter[AlignmentRecord](AlignmentRecord.SCHEMA$)

    val longString = "IAMAREALLYLONGSTRINGTHATSHOULDNTBECACHED"
    assert(longString.length > maxCacheableStringLength)
    val sequences = Array.tabulate(cacheCapacity * 3 + 1) {
      i => if (i % 2 == 0) s"${i}" else longString
    }
    val shortStrings = sequences.filter(_.length < maxCacheableStringLength)

    val recordsIn = Array.tabulate(cacheCapacity * 3 + 1) {
      i =>
        {
          AlignmentRecord.newBuilder()
            .setSequence(sequences(i))
            .build()
        }
    }

    for (record <- recordsIn) {
      writer.write(record, encoder)
    }

    val inputStream = new FastByteArrayInputStream(outstream.array)
    val decoder = new BinaryDecoderWithLRUCache(inputStream, maxCacheableStringLength, cache)
    val reader = new SpecificDatumReader[AlignmentRecord](AlignmentRecord.SCHEMA$)

    val recordsOut = Array.tabulate(cacheCapacity * 3 + 1) {
      i =>
        {
          reader.read(null.asInstanceOf[AlignmentRecord], decoder)
        }
    }

    assert(recordsIn sameElements recordsOut)
    assert(cache.size() == cacheCapacity)
    assert(!cache.containsKey(longString))
    val (notInCache, inCache) = shortStrings.splitAt(cacheCapacity * 3 - cacheCapacity)
    notInCache.foreach(str => assert(!cache.containsKey(str)))
    inCache.foreach(str => assert(cache.containsKey(str)))
  }

}
