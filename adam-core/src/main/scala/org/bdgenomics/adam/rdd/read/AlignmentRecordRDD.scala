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
package org.bdgenomics.adam.rdd.read

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ AvroReadGroupGenomicRDD, Unaligned }
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig, RecordGroupMetadata }

trait AlignmentRecordRDD extends AvroReadGroupGenomicRDD[AlignmentRecord, AlignmentRecordRDD] {

  def toFragments: FragmentRDD = {
    FragmentRDD(rdd.toFragments,
      sequences,
      recordGroups)
  }

  protected def getReferenceRegions(elem: AlignmentRecord): Seq[ReferenceRegion] = {
    ReferenceRegion.opt(elem).toSeq
  }
}

case class AlignedReadRDD(rdd: RDD[AlignmentRecord],
                          sequences: SequenceDictionary,
                          recordGroups: RecordGroupDictionary) extends AlignmentRecordRDD {

  protected def replaceRdd(newRdd: RDD[AlignmentRecord]): AlignedReadRDD = {
    copy(rdd = newRdd)
  }
}

object UnalignedReadRDD {

  /**
   * Creates an unaligned read RDD where no record groups are attached.
   *
   * @param rdd RDD of reads.
   * @return Returns an unaligned read RDD with an empty record group dictionary.
   */
  private[rdd] def fromRdd(rdd: RDD[AlignmentRecord]): UnalignedReadRDD = {
    UnalignedReadRDD(rdd, RecordGroupDictionary.empty)
  }
}

case class UnalignedReadRDD(rdd: RDD[AlignmentRecord],
                            recordGroups: RecordGroupDictionary) extends AlignmentRecordRDD
    with Unaligned {

  protected def replaceRdd(newRdd: RDD[AlignmentRecord]): UnalignedReadRDD = {
    copy(rdd = newRdd)
  }
}

