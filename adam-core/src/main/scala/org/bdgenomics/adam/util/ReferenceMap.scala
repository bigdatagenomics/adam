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

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.Slice

/**
 * A broadcastable ReferenceFile backed by a map containing reference name ->
 * Seq[Slice] pairs.
 *
 * @param referenceMap a map containing a Seq of slices per contig.
 */
case class ReferenceMap(referenceMap: Map[String, Seq[Slice]]) extends ReferenceFile {

  private def keys(): String = {
    referenceMap.keys.toList.sortBy(x => x).mkString(", ")
  }

  override def toString(): String = {
    "ReferenceMap(%s)".format(keys())
  }

  /**
   * The reference sequence dictionary corresponding to the contigs in this collection of fragments.
   */
  val references: SequenceDictionary = new SequenceDictionary(referenceMap.map(r =>
    SequenceRecord(r._1, r._2.map(_.getEnd).max)).toVector)

  /**
   * Extract reference sequence from the file.
   *
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  override def extract(region: ReferenceRegion): String = {
    referenceMap
      .getOrElse(
        region.referenceName,
        throw new Exception(
          "Reference %s not found in reference map with keys: %s".format(region.referenceName, keys())
        )
      )
      .dropWhile(s => s.getStart + s.getSequence.length < region.start)
      .takeWhile(_.getStart < region.end)
      .map(
        clipFragment(_, region.start, region.end)
      )
      .mkString("")
  }

  private def clipFragment(slice: Slice, start: Long, end: Long): String = {
    val min =
      math.max(
        0L,
        start - slice.getStart
      ).toInt

    val max =
      math.min(
        slice.getSequence.length,
        end - slice.getStart
      ).toInt

    slice.getSequence.substring(min, max)
  }
}

/**
 * Companion object for creating a ReferenceMap from an RDD of slices.
 */
object ReferenceMap {

  /**
   * Builds a ReferenceMap from an RDD of slices.
   *
   * @param slices RDD of slices describing a genome reference.
   * @return Returns a serializable wrapper around these slices that enables
   *   random access into the reference genome.
   */
  def apply(slices: RDD[Slice]): ReferenceMap = {
    ReferenceMap(
      slices
        .groupBy(_.getName)
        .mapValues(_.toSeq.sortBy(_.getStart))
        .collectAsMap
        .toMap
    )
  }
}

class ReferenceMapSerializer extends Serializer[ReferenceMap] {
  private val sliceSerializer = new AvroSerializer[Slice]

  def write(kryo: Kryo, out: Output, record: ReferenceMap) = {
    out.writeInt(record.referenceMap.size)
    record.referenceMap.foreach(p => {
      out.writeString(p._1)
      out.writeInt(p._2.size)
      p._2.foreach(slice => {
        sliceSerializer.write(kryo, out, slice)
      })
    })
  }

  def read(kryo: Kryo, in: Input, clazz: Class[ReferenceMap]): ReferenceMap = {
    val n = in.readInt()
    val array = new Array[(String, Seq[Slice])](n)
    (0 until n).foreach(idx => {
      val key = in.readString()
      val numSlices = in.readInt()
      val sliceArray = new Array[Slice](numSlices)
      (0 until numSlices).foreach(jdx => {
        sliceArray(jdx) = sliceSerializer.read(kryo, in, classOf[Slice])
      })
      array(idx) = (key, sliceArray.toSeq)
    })
    ReferenceMap(array.toMap)
  }
}
