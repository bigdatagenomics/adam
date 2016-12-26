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

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.SupportedHeaderLines
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.{
  AvroGenomicRDD,
  JavaSaveArgs,
  VCFHeaderUtils
}
import org.bdgenomics.formats.avro.{
  Contig,
  Sample,
  Variant
}
import scala.collection.JavaConversions._

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

  override protected def saveMetadata(filePath: String) {

    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration)

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }

  /**
   * Java-friendly method for saving to Parquet.
   *
   * @param filePath Path to save to.
   */
  def save(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }

  /**
   * Explicitly saves to VCF.
   *
   * @param filePath The filepath to save to.
   * @param asSingleFile If true, saves the output as a single file by merging
   *   the sharded output after completing the write to HDFS. If false, the
   *   output of this call will be written as shards, where each shard has a
   *   valid VCF header.
   * @param stringency The validation stringency to use when writing the VCF.
   */
  def saveAsVcf(filePath: String,
                asSingleFile: Boolean,
                stringency: ValidationStringency) {
    toVariantContextRDD.saveAsVcf(filePath, asSingleFile, stringency)
  }

  /**
   * @return Returns this VariantRDD as a VariantContextRDD.
   */
  def toVariantContextRDD: VariantContextRDD = {
    VariantContextRDD(rdd.map(VariantContext(_)), sequences, Seq.empty[Sample], headerLines)
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
