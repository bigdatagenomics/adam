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

import java.util.Date
import net.sf.samtools.{ SAMFileReader, SAMFileHeader, SAMReadGroupRecord }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.ADAMRecord
import scala.collection.JavaConversions._

object RecordGroupDictionary {

  /**
   * Builds a record group dictionary from a SAM file header.
   *
   * @param header SAM file header with attached read groups.
   * @return Returns a new record group dictionary with the read groups attached to the file header.
   */
  def fromSAMHeader(header: SAMFileHeader): RecordGroupDictionary = {
    // force implicit conversion
    val readGroups: List[SAMReadGroupRecord] = header.getReadGroups
    new RecordGroupDictionary(readGroups.map(RecordGroup(_)))
  }

  /**
   * Builds a record group dictionary from a SAM file reader by collecting the header, and the
   * read groups attached to the header.
   *
   * @param header SAM file header with attached read groups.
   * @return Returns a new record group dictionary with the read groups attached to the file header.
   */
  def fromSAMReader(samReader: SAMFileReader): RecordGroupDictionary = {
    fromSAMHeader(samReader.getFileHeader)
  }

}

/**
 * Builds a dictionary containing record groups. Record groups must have a unique name across all
 * samples in the dictionary. This dictionary provides numerical IDs for each group; these IDs
 * are only consistent when referencing a single dictionary.
 *
 * @param recordGroups A seq of record groups to popualate the dictionary.
 *
 * @throws AssertionError Throws an assertion error if there are multiple record groups with the
 * same name.
 */
class RecordGroupDictionary(val recordGroups: Seq[RecordGroup]) extends Serializable {
  lazy val recordGroupMap = recordGroups.map(v => (v.recordGroupName, v))
    .sortBy(kv => kv._1)
    .zipWithIndex
    .map(kv => {
      val ((name, group), index) = kv
      (name, (group, index))
    }).toMap

  assert(recordGroupMap.size == recordGroups.length,
    "Read group dictionary contains multiple samples with identical read group names.")

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
}

object RecordGroup {
  /**
   * Creates a record group from read data. Returns a None option if the record group information
   * is not populated in the read.
   *
   * @param read Read to populate data from.
   * @return Returns a filled Option if record group data is attached to the read, else returns None.
   */
  def apply(read: ADAMRecord): Option[RecordGroup] = {
    Option(read.getRecordGroupSample).fold(None.asInstanceOf[Option[RecordGroup]])(rgs => {
      Option(read.getRecordGroupName).fold(None.asInstanceOf[Option[RecordGroup]])(rgn => {
        Some(new RecordGroup(rgs.toString,
          rgn.toString,
          Option(read.getRecordGroupSequencingCenter).map(_.toString),
          Option(read.getRecordGroupDescription).map(_.toString),
          Option(read.getRecordGroupRunDateEpoch).map(_.toLong),
          Option(read.getRecordGroupFlowOrder).map(_.toString),
          Option(read.getRecordGroupKeySequence).map(_.toString),
          Option(read.getRecordGroupLibrary).map(_.toString),
          Option(read.getRecordGroupPredictedMedianInsertSize).map(_.toInt),
          Option(read.getRecordGroupPlatform).map(_.toString),
          Option(read.getRecordGroupPlatformUnit).map(_.toString)))
      })
    })
  }

  /**
   * Converts a SAMReadGroupRecord into a RecordGroup.
   *
   * @param samRGR SAMReadGroupRecord to convert.
   * @return Returns an equivalent ADAM format record group.
   */
  def apply(samRGR: SAMReadGroupRecord): RecordGroup = {
    assert(samRGR.getSample != null,
      "Sample ID is not set for read group " + samRGR.getReadGroupId)
    new RecordGroup(samRGR.getSample,
      samRGR.getReadGroupId,
      Option(samRGR.getSequencingCenter).map(_.toString),
      Option(samRGR.getDescription).map(_.toString),
      Option(samRGR.getRunDate).map(_.getTime),
      Option(samRGR.getFlowOrder).map(_.toString),
      Option(samRGR.getKeySequence).map(_.toString),
      Option(samRGR.getLibrary).map(_.toString),
      Option({
        // must explicitly reference as a java.lang.integer to avoid implicit conversion
        val i: java.lang.Integer = samRGR.getPredictedMedianInsertSize
        i
      }).map(_.toInt),
      Option(samRGR.getPlatform).map(_.toString),
      Option(samRGR.getPlatformUnit).map(_.toString))
  }
}

class RecordGroup(val sample: String,
                  val recordGroupName: String,
                  val sequencingCenter: Option[String] = None,
                  val description: Option[String] = None,
                  val runDateEpoch: Option[Long] = None,
                  val flowOrder: Option[String] = None,
                  val keySequence: Option[String] = None,
                  val library: Option[String] = None,
                  val predictedMedianInsertSize: Option[Int] = None,
                  val platform: Option[String] = None,
                  val platformUnit: Option[String] = None) extends Serializable {

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
