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
package org.bdgenomics.adam.rdd.fragment

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.AvroReadGroupGenomicRDD
import org.bdgenomics.adam.rdd.read.{
  AlignedReadRDD,
  AlignmentRecordRDD,
  UnalignedReadRDD
}
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._

object FragmentRDD {

  /**
   * Creates a FragmentRDD where no record groups or sequence info are attached.
   *
   * @param rdd RDD of fragments.
   * @return Returns a FragmentRDD with an empty record group dictionary and sequence dictionary.
   */
  private[rdd] def fromRdd(rdd: RDD[Fragment]): FragmentRDD = {
    FragmentRDD(rdd, SequenceDictionary.empty, RecordGroupDictionary.empty)
  }
}

case class FragmentRDD(rdd: RDD[Fragment],
                       sequences: SequenceDictionary,
                       recordGroups: RecordGroupDictionary) extends AvroReadGroupGenomicRDD[Fragment, FragmentRDD] {

  protected def replaceRdd(newRdd: RDD[Fragment]): FragmentRDD = {
    copy(rdd = newRdd)
  }

  def toReads: AlignmentRecordRDD = {
    val converter = new AlignmentRecordConverter

    // convert the fragments to reads
    val newRdd = rdd.flatMap(converter.convertFragment)

    // are we aligned?
    if (sequences.isEmpty) {
      UnalignedReadRDD(newRdd, recordGroups)
    } else {
      AlignedReadRDD(newRdd, sequences, recordGroups)
    }
  }

  protected def getReferenceRegions(elem: Fragment): Seq[ReferenceRegion] = {
    elem.getAlignments
      .flatMap(r => ReferenceRegion.opt(r))
      .toSeq
  }
}
