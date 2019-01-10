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
package org.bdgenomics.adam.models

import htsjdk.samtools.{ SAMFileHeader, SAMReadGroupRecord }
import java.util.Date
import org.bdgenomics.formats.avro.{ ReadGroup => ReadGroupMetadata, Sample }
import scala.collection.JavaConversions._

/**
 * Singleton object for creating dictionaries of read groups.
 */
object ReadGroupDictionary {

  /**
   * Builds a read group dictionary from a SAM file header.
   *
   * @param header SAM file header with attached read groups.
   * @return Returns a new read group dictionary with the read groups attached to the file header.
   */
  def fromSAMHeader(header: SAMFileHeader): ReadGroupDictionary = {
    val readGroups = header.getReadGroups
    new ReadGroupDictionary(readGroups.map(ReadGroup(_)))
  }

  /**
   * @return Returns a read group dictionary that contains no read groups.
   */
  def empty: ReadGroupDictionary = new ReadGroupDictionary(Seq.empty)
}

/**
 * Builds a dictionary containing read groups.
 *
 * Read groups must have a unique name across all samples in the dictionary.
 * This dictionary provides numerical IDs for each group; these IDs are only
 * consistent when referencing a single dictionary.
 *
 * @param readGroups A seq of read groups to populate the dictionary.
 *
 * @throws IllegalArgumentException Throws an assertion error if there are multiple read
 *   groups with the same name.
 */
case class ReadGroupDictionary(readGroups: Seq[ReadGroup]) {

  /**
   * The number of read groups in the dictionary.
   */
  def size: Int = readGroups.size

  /**
   * @return Returns true if this dictionary contains no read groups.
   */
  def isEmpty: Boolean = {
    readGroups.isEmpty
  }

  /**
   * A representation of this read group dictionary that can be indexed into
   * by read group id.
   */
  val readGroupMap = readGroups.map(v => (v.id, v))
    .sortBy(_._1)
    .zipWithIndex
    .map(kv => {
      val ((name, group), index) = kv
      (name, (group, index))
    }).toMap

  require(
    readGroupMap.size == readGroups.length,
    "Read group dictionary contains multiple samples with identical read group ids."
  )

  /**
   * Merges together two read group dictionaries.
   *
   * @param that The read group dictionary to merge with.
   * @return The merged read group dictionary.
   */
  def ++(that: ReadGroupDictionary): ReadGroupDictionary = {
    new ReadGroupDictionary((readGroups ++ that.readGroups).distinct)
  }

  /**
   * @return Converts this read group dictionary to a set of samples.
   */
  def toSamples: Seq[Sample] = {
    readGroups.map(_.sampleId)
      .distinct
      .map(s => {
        Sample.newBuilder()
          .setId(s)
          .build()
      })
  }

  /**
   * Returns the numerical index for a given read group id.
   *
   * @note This index is only guaranteed to have the same value for a given read group id
   * when the index is pulled from the same read group dictionary.
   *
   * @param readGroupId The read group to find an index for.
   * @return Returns a numerical index for a read group.
   */
  def getIndex(readGroupId: String): Int = {
    readGroupMap(readGroupId)._2
  }

  /**
   * Returns the read group entry for an associated id.
   *
   * @param readGroupId The read group to look up.
   * @return Detailed info for a read group.
   */
  def apply(readGroupId: String): ReadGroup = {
    readGroupMap(readGroupId)._1
  }

  override def toString: String = {
    "ReadGroupDictionary(%s)".format(readGroups.mkString(","))
  }
}

/**
 * Singleton object for creating ReaGroups.
 */
object ReadGroup {

  /**
   * Converts a SAMReadGroupRecord into a ReadGroup.
   *
   * @param samRGR SAMReadGroupRecord to convert.
   * @return Returns an equivalent ADAM format read group.
   */
  def apply(samRGR: SAMReadGroupRecord): ReadGroup = {
    require(
      samRGR.getSample != null,
      "Sample ID is not set for read group %s".format(samRGR.getReadGroupId)
    )
    new ReadGroup(
      samRGR.getSample,
      samRGR.getReadGroupId,
      Option(samRGR.getSequencingCenter),
      Option(samRGR.getDescription),
      Option(samRGR.getRunDate).map(_.getTime),
      Option(samRGR.getFlowOrder),
      Option(samRGR.getKeySequence),
      Option(samRGR.getLibrary),
      Option({
        // must explicitly reference as a java.lang.integer to avoid implicit conversion
        samRGR.getPredictedMedianInsertSize: java.lang.Integer
      }).map(_.toInt),
      Option(samRGR.getPlatform),
      Option(samRGR.getPlatformUnit)
    )
  }

  /**
   * Converts the Avro ReadGroup record into a model ReadGroup.
   *
   * Although the ReadGroup class is serializable, when saving the record to
   * disk, we want to save it as Avro. This method allows us to go from the
   * Avro format which we use on disk into the class we use in memory.
   *
   * @param readGroup ReadGroup record loaded from disk.
   * @return Avro record converted into ReadGroup representation.
   */
  def fromAvro(readGroup: ReadGroupMetadata): ReadGroup = {
    require(readGroup.getId != null, "Read group id is null in %s.".format(readGroup))
    require(readGroup.getSampleId != null, "Read group sampleId is null in %s.".format(readGroup))

    new ReadGroup(readGroup.getSampleId,
      readGroup.getId,
      Option(readGroup.getSequencingCenter),
      Option(readGroup.getDescription),
      Option({
        // must explicitly reference as a java.lang.integer to avoid implicit conversion
        val l: java.lang.Long = readGroup.getRunDateEpoch
        l
      }).map(_.toLong),
      Option(readGroup.getFlowOrder),
      Option(readGroup.getKeySequence),
      Option(readGroup.getLibrary),
      Option({
        // must explicitly reference as a java.lang.integer to avoid implicit conversion
        val i: java.lang.Integer = readGroup.getPredictedMedianInsertSize
        i
      }).map(_.toInt),
      Option(readGroup.getPlatform),
      Option(readGroup.getPlatformUnit))
  }
}

/**
 * A read group represents a set of reads that were
 * sequenced/processed/prepped/analyzed together.
 *
 * @param sampleId The sample these reads are from.
 * @param id The identifier for this read group.
 * @param sequencingCenter The optional name of the place where these reads
 *   were sequenced.
 * @param description An optional description for this read group.
 * @param runDateEpoch An optional Unix epoch timestamp for when these reads
 *   were run through the sequencer.
 * @param flowOrder An optional string of nucleotides that were used for each
 *   flow of each read.
 * @param keySequence An optional string of nucleotides that are the key for
 *   this read.
 * @param library An optional library name.
 * @param predictedMedianInsertSize An optional prediction of the read insert
 *   size for this library prep.
 * @param platform An optional description for the platform this group was
 *   sequenced on.
 * @param platformUnit An optional ID for the sequencer this group was sequenced
 *   on.
 */
case class ReadGroup(
    sampleId: String,
    id: String,
    sequencingCenter: Option[String] = None,
    description: Option[String] = None,
    runDateEpoch: Option[Long] = None,
    flowOrder: Option[String] = None,
    keySequence: Option[String] = None,
    library: Option[String] = None,
    predictedMedianInsertSize: Option[Int] = None,
    platform: Option[String] = None,
    platformUnit: Option[String] = None) {

  /**
   * Compares equality to another object. Only checks equality via the sampleId and
   * id fields.
   *
   * @param o Object to compare against.
   * @return Returns true if the object is a ReadGroup, and the sampleId and id fields are
   * equal. Else, returns false.
   */
  override def equals(o: Any): Boolean = o match {
    case rg: ReadGroup => rg.sampleId == sampleId && rg.id == id
    case _             => false
  }

  /**
   * Generates a hash from the sampleId and id fields.
   *
   * @return Hash code for this object.
   */
  override def hashCode(): Int = (sampleId + id).hashCode()

  /**
   * Converts this into an Avro ReadGroup description for
   * serialization to disk.
   *
   * @return Returns Avro version of ReadGroup.
   */
  def toMetadata: ReadGroupMetadata = {
    // make builder and set required fields
    val builder = ReadGroupMetadata.newBuilder()
      .setId(id)
      .setSampleId(sampleId)

    // set optional fields
    sequencingCenter.foreach(v => builder.setSequencingCenter(v))
    description.foreach(v => builder.setDescription(v))
    runDateEpoch.foreach(v => builder.setRunDateEpoch(v))
    flowOrder.foreach(v => builder.setFlowOrder(v))
    keySequence.foreach(v => builder.setKeySequence(v))
    library.foreach(v => builder.setLibrary(v))
    predictedMedianInsertSize.foreach(v => builder.setPredictedMedianInsertSize(v))
    platform.foreach(v => builder.setPlatform(v))
    platformUnit.foreach(v => builder.setPlatformUnit(v))

    builder.build()
  }

  /**
   * Converts a read group into a SAM formatted read group.
   *
   * @return A SAM formatted read group.
   */
  def toSAMReadGroupRecord(): SAMReadGroupRecord = {
    val rgr = new SAMReadGroupRecord(id)

    // set fields
    rgr.setSample(sampleId)
    sequencingCenter.foreach(rgr.setSequencingCenter)
    description.foreach(rgr.setDescription)
    runDateEpoch.foreach(e => rgr.setRunDate(new Date(e)))
    flowOrder.foreach(rgr.setFlowOrder)
    keySequence.foreach(rgr.setKeySequence)
    library.foreach(rgr.setLibrary)
    predictedMedianInsertSize.foreach(is => {
      // force implicit conversion
      val insertSize: java.lang.Integer = is
      rgr.setPredictedMedianInsertSize(insertSize)
    })
    platform.foreach(rgr.setPlatform)
    platformUnit.foreach(rgr.setPlatformUnit)

    // return
    rgr
  }
}
