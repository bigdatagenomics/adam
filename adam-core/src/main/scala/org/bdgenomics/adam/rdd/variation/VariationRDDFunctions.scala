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
import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.rich.RichGenotype._
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.formats.avro.{ Genotype, GenotypeType, DatabaseVariantAnnotation }
import org.seqdoop.hadoop_bam._

class VariantContextRDDFunctions(rdd: RDD[VariantContext]) extends ADAMSequenceDictionaryRDDAggregator[VariantContext](rdd) with Logging {

  /**
   * For a single variant context, returns sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecordsFromElement(elem: VariantContext): scala.collection.Set[SequenceRecord] = {
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

  def getCallsetSamples(): List[String] = {
    rdd.flatMap(c => c.genotypes.map(_.getSampleId).toSeq.distinct)
      .distinct
      .map(_.toString)
      .collect()
      .toList
  }

  /**
   * Converts an RDD of ADAM VariantContexts to HTSJDK VariantContexts
   * and saves to disk as VCF.
   *
   * @param filePath The filepath to save to.
   * @param dict An optional sequence dictionary. Default is none.
   * @param sortOnSave Whether to sort before saving. Sort is run after coalescing.
   *                   Default is false (no sort).
   * @param coalesceTo Optionally coalesces the RDD down to _n_ partitions. Default is none.
   */
  def saveAsVcf(filePath: String,
                dict: Option[SequenceDictionary] = None,
                sortOnSave: Boolean = false,
                coalesceTo: Option[Int] = None) = {
    val vcfFormat = VCFFormat.inferFromFilePath(filePath)
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported") // TODO: Add BCF support

    rdd.cache()
    log.info("Writing %s file to %s".format(vcfFormat, filePath))

    // Initialize global header object required by Hadoop VCF Writer
    val header = getCallsetSamples()
    val bcastHeader = rdd.context.broadcast(header)
    val mp = rdd.mapPartitionsWithIndex((idx, iter) => {
      log.warn("Setting header for partition " + idx)
      synchronized {
        // perform map partition call to ensure that the VCF header is set on all
        // nodes in the cluster; see:
        // https://github.com/bigdatagenomics/adam/issues/353
        ADAMVCFOutputFormat.setHeader(bcastHeader.value)
        log.warn("Set VCF header for partition " + idx)
      }
      Iterator[Int]()
    }).count()

    // force value check, ensure that computation happens
    if (mp != 0) {
      log.warn("Had more than 0 elements after map partitions call to set VCF header across cluster.")
    }

    // convert the variants to htsjdk vc
    val converter = new VariantContextConverter(dict)
    val gatkVCs: RDD[VariantContextWritable] = rdd.map(v => {
      val vcw = new VariantContextWritable
      vcw.set(converter.convert(v))
      vcw
    })

    // coalesce the rdd if requested
    val coalescedVCs = coalesceTo.fold(gatkVCs)(p => gatkVCs.repartition(p))

    // sort if requested
    val withKey = if (sortOnSave) {
      coalescedVCs.keyBy(v => ReferencePosition(v.get.getChr(), v.get.getStart()))
        .sortByKey()
        .map(kv => (new LongWritable(kv._1.pos), kv._2))
    } else {
      coalescedVCs.keyBy(v => new LongWritable(v.get.getStart))
    }

    // save to disk
    val conf = rdd.context.hadoopConfiguration
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)
    withKey.saveAsNewAPIHadoopFile(filePath,
      classOf[LongWritable], classOf[VariantContextWritable], classOf[ADAMVCFOutputFormat[LongWritable]],
      conf)

    log.info("Write %d records".format(gatkVCs.count()))
    rdd.unpersist()
  }
}

class GenotypeRDDFunctions(rdd: RDD[Genotype]) extends Serializable with Logging {
  def toVariantContext(): RDD[VariantContext] = {
    rdd.keyBy({ g => RichVariant.variantToRichVariant(g.getVariant) })
      .groupByKey
      .map { case (v: RichVariant, g) => new VariantContext(v, g, None) }
  }

  def filterByOverlappingRegion(query: ReferenceRegion): RDD[Genotype] = {
    def overlapsQuery(rec: Genotype): Boolean =
      rec.getVariant.getContig.getContigName.toString == query.referenceName &&
        rec.getVariant.getStart < query.end &&
        rec.getVariant.getEnd > query.start
    rdd.filter(overlapsQuery)
  }
}
