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
import org.bdgenomics.formats.avro.{ RecordGroup => RecordGroupMetadata, Sample }
import scala.collection.JavaConversions._

/**
 * Singleton object for creating dictionaries of record groups.
 */
object RecordGroupDictionary {

  /**
   * Builds a record group dictionary from a SAM file header.
   *
   * @param header SAM file header with attached read groups.
   * @return Returns a new record group dictionary with the read groups attached to the file header.
   */
  def fromSAMHeader(header: SAMFileHeader): RecordGroupDictionary = {
    val readGroups = header.getReadGroups
    new RecordGroupDictionary(readGroups.map(RecordGroup(_)))
  }

  /**
   * @return Returns a record group dictionary that contains no record groups.
   */
  def empty: RecordGroupDictionary = new RecordGroupDictionary(Seq.empty)
}

/**
 * Builds a dictionary containing record groups.
 *
 * Record groups must have a unique name across all samples in the dictionary.
 * This dictionary provides numerical IDs for each group; these IDs are only
 * consistent when referencing a single dictionary.
 *
 * @param recordGroups A seq of record groups to populate the dictionary.
 *
 * @throws IllegalArgumentError Throws an assertion error if there are multiple record
 *   groups with the same name.
 */
case class RecordGroupDictionary(recordGroups: Seq[RecordGroup]) {

  /**
   * The number of record groups in the dictionary.
   */
  def size: Int = recordGroups.size

  /**
   * @return Returns true if this dictionary contains no record groups.
   */
  def isEmpty: Boolean = {
    recordGroups.isEmpty
  }

  /**
   * A representation of this record group dictionary that can be indexed into
   * by record group name.
   */
  val recordGroupMap = recordGroups.map(v => (v.recordGroupName, v))
    .sortBy(_._1)
    .zipWithIndex
    .map(kv => {
      val ((name, group), index) = kv
      (name, (group, index))
    }).toMap

  require(
    recordGroupMap.size == recordGroups.length,
    "Read group dictionary contains multiple samples with identical read group names."
  )

  /**
   * Merges together two record group dictionaries.
   *
   * @param that The record group dictionary to merge with.
   * @return The merged record group dictionary.
   */
  def ++(that: RecordGroupDictionary): RecordGroupDictionary = {
    new RecordGroupDictionary((recordGroups ++ that.recordGroups).distinct)
  }

  /**
   * @return Converts this record group dictionary to a set of samples.
   */
  def toSamples: Seq[Sample] = {
    recordGroups.map(_.sample)
      .distinct
      .map(s => {
        Sample.newBuilder()
          .setSampleId(s)
          .build()
      })
  }

  /**
   * Returns the numerical index for a given record group name.
   *
   * @note This index is only guaranteed to have the same value for a given record group name
   * when the index is pulled from the same record group dictionary.
   *
   * @param recordGroupName The record group to find an index for.
   * @return Returns a numerical index for a record group.
   */
  def getIndex(recordGroupName: String): Int = {
    recordGroupMap(recordGroupName)._2
  }

  /**
   * Returns the record group entry for an associated name.
   *
   * @param recordGroupName The record group to look up.
   * @return Detailed info for a record group.
   */
  def apply(recordGroupName: String): RecordGroup = {
    recordGroupMap(recordGroupName)._1
  }

  override def toString: String = {
    "RecordGroupDictionary(%s)".format(recordGroups.mkString(","))
  }
}

/**
 * Singleton object for creating RecordGroups.
 */
object RecordGroup {

  /**
   * Converts a SAMReadGroupRecord into a RecordGroup.
   *
   * @param samRGR SAMReadGroupRecord to convert.
   * @return Returns an equivalent ADAM format record group.
   */
  def apply(samRGR: SAMReadGroupRecord): RecordGroup = {
    require(
      samRGR.getSample != null,
      "Sample ID is not set for read group %s".format(samRGR.getReadGroupId)
    )
    new RecordGroup(
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
   * Converts the Avro RecordGroupMetadata record into a RecordGroup.
   *
   * Although the RecordGroup class is serializable, when saving the record to
   * disk, we want to save it as Avro. This method allows us to go from the
   * Avro format which we use on disk into the class we use in memory.
   *
   * @param rgm RecordGroupMetadata record loaded from disk.
   * @return Avro record converted into RecordGroup representation.
   */
  def fromAvro(rgm: RecordGroupMetadata): RecordGroup = {
    require(rgm.getName != null, "Record group name is null in %s.".format(rgm))
    require(rgm.getSample != null, "Record group sample is null in %s.".format(rgm))

    new RecordGroup(rgm.getSample,
      rgm.getName,
      Option(rgm.getSequencingCenter),
      Option(rgm.getDescription),
      Option({
        // must explicitly reference as a java.lang.integer to avoid implicit conversion
        val l: java.lang.Long = rgm.getRunDateEpoch
        l
      }).map(_.toLong),
      Option(rgm.getFlowOrder),
      Option(rgm.getKeySequence),
      Option(rgm.getLibrary),
      Option({
        // must explicitly reference as a java.lang.integer to avoid implicit conversion
        val i: java.lang.Integer = rgm.getPredictedMedianInsertSize
        i
      }).map(_.toInt),
      Option(rgm.getPlatform),
      Option(rgm.getPlatformUnit))
  }
}

/**
 * A record group represents a set of reads that were
 * sequenced/processed/prepped/analyzed together.
 *
 * @param sample The sample these reads are from.
 * @param recordGroupName The name of this record group.
 * @param sequencingCenter The optional name of the place where these reads
 *   were sequenced.
 * @param description An optional description for this record group.
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
case class RecordGroup(
    sample: String,
    recordGroupName: String,
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
   * Compares equality to another object. Only checks equality via the sample and
   * recordGroupName fields.
   *
   * @param o Object to compare against.
   * @return Returns true if the object is a RecordGroup, and the sample and group name are
   * equal. Else, returns false.
   */
  override def equals(o: Any): Boolean = o match {
    case rg: RecordGroup => rg.sample == sample && rg.recordGroupName == recordGroupName
    case _               => false
  }

  /**
   * Generates a hash from the sample and record group name fields.
   *
   * @return Hash code for this object.
   */
  override def hashCode(): Int = (sample + recordGroupName).hashCode()

  /**
   * Converts this into an Avro RecordGroupMetadata description for
   * serialization to disk.
   *
   * @return Returns Avro version of RecordGroup.
   */
  def toMetadata: RecordGroupMetadata = {
    // make builder and set required fields
    val builder = RecordGroupMetadata.newBuilder()
      .setName(recordGroupName)
      .setSample(sample)

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
   * Converts a record group into a SAM formatted record group.
   *
   * @return A SAM formatted record group.
   */
  def toSAMReadGroupRecord(): SAMReadGroupRecord = {
    val rgr = new SAMReadGroupRecord(recordGroupName)

    // set fields
    rgr.setSample(sample)
    sequencingCenter.foreach(rgr.setSequencingCenter)
    description.foreach(rgr.setDescription)
    runDateEpoch.foreach(e => rgr.setRunDate(new Date(e)))
    flowOrder.foreach(rgr.setFlowOrder)
    keySequence.foreach(rgr.setFlowOrder)
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
