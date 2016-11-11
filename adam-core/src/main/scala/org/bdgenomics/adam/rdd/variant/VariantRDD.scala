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
package org.bdgenomics.adam.rdd.variant

import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.SupportedHeaderLines
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.{ AvroGenomicRDD, JavaSaveArgs }
import org.bdgenomics.formats.avro.Variant

/**
 * An RDD containing variants called against a given reference genome.
 *
 * @param rdd Variants.
 * @param sequences A dictionary describing the reference genome.
 * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
 *   needed to represent this RDD of Variants.
 */
case class VariantRDD(rdd: RDD[Variant],
                      sequences: SequenceDictionary,
                      @transient headerLines: Seq[VCFHeaderLine] = SupportedHeaderLines.allHeaderLines) extends AvroGenomicRDD[Variant, VariantRDD] {

  /**
   * Java-friendly method for saving to Parquet.
   *
   * @param filePath Path to save to.
   */
  def save(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }

  /**
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new VariantRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Variant]): VariantRDD = {
    copy(rdd = newRdd)
  }

  /**
   * @param elem The variant to get a reference region for.
   * @return Returns the singular region this variant covers.
   */
  protected def getReferenceRegions(elem: Variant): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
