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
package org.bdgenomics.adam.rdd.variation

import org.apache.hadoop.io.LongWritable
import org.bdgenomics.utils.misc.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferencePosition, ReferenceRegion, SequenceDictionary, SequenceRecord, VariantContext }
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.{ DatabaseVariantAnnotation, Genotype }

class VariantContextRDDFunctions(rdd: RDD[VariantContext]) extends ADAMSequenceDictionaryRDDAggregator[VariantContext](rdd) with Logging {

  /**
   * For a single variant context, returns sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecords(elem: VariantContext): Set[SequenceRecord] = {
    elem.genotypes.map(gt => SequenceRecord.fromSpecificRecord(gt.getVariant)).toSet
  }

  /**
   * Left outer join database variant annotations
   *
   */
  def joinDatabaseVariantAnnotation(ann: RDD[DatabaseVariantAnnotation]): RDD[VariantContext] = {
    rdd.keyBy(_.variant)
      .leftOuterJoin(ann.keyBy(_.getVariant))
      .values
      .map { case (v: VariantContext, a) => VariantContext(v.variant, v.genotypes, a) }

  }
}
