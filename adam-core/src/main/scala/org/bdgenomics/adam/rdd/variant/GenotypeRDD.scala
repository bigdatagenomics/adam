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
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.MultisampleAvroGenomicRDD
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.formats.avro.{ Genotype, Sample }
import scala.reflect.ClassTag

private[adam] case class GenotypeArray(
    array: Array[(ReferenceRegion, Genotype)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Genotype] {

  def duplicate(): IntervalArray[ReferenceRegion, Genotype] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Genotype)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Genotype] = {
    GenotypeArray(arr, maxWidth)
  }
}

private[adam] class GenotypeArraySerializer extends IntervalArraySerializer[ReferenceRegion, Genotype, GenotypeArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Genotype]

  protected def builder(arr: Array[(ReferenceRegion, Genotype)],
                        maxIntervalWidth: Long): GenotypeArray = {
    GenotypeArray(arr, maxIntervalWidth)
  }
}

object GenotypeRDD extends Serializable {

  /**
   * Builds a GenotypeRDD without a partition map.
   *
   * @param rdd The underlying RDD.
   * @param sequences The sequence dictionary for the RDD.
   * @param samples The samples for the RDD.
   * @param headerLines The header lines for the RDD.
   * @return A new GenotypeRDD.
   */
  def apply(rdd: RDD[Genotype],
            sequences: SequenceDictionary,
            samples: Seq[Sample],
            headerLines: Seq[VCFHeaderLine]): GenotypeRDD = {
    GenotypeRDD(rdd, sequences, samples, headerLines, None)
  }
}

/**
 * An RDD containing genotypes called in a set of samples against a given
 * reference genome.
 *
 * @param rdd Called genotypes.
 * @param sequences A dictionary describing the reference genome.
 * @param samples The samples called.
 * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
 *   needed to represent this RDD of Genotypes.
 */
case class GenotypeRDD(rdd: RDD[Genotype],
                       sequences: SequenceDictionary,
                       @transient samples: Seq[Sample],
                       @transient headerLines: Seq[VCFHeaderLine],
                       optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends MultisampleAvroGenomicRDD[Genotype, GenotypeRDD] {

  def union(rdds: GenotypeRDD*): GenotypeRDD = {
    val iterableRdds = rdds.toSeq
    GenotypeRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      (samples ++ iterableRdds.flatMap(_.samples)).distinct,
      (headerLines ++ iterableRdds.flatMap(_.headerLines)).distinct)
  }

  protected def buildTree(rdd: RDD[(ReferenceRegion, Genotype)])(
    implicit tTag: ClassTag[Genotype]): IntervalArray[ReferenceRegion, Genotype] = {
    IntervalArray(rdd, GenotypeArray.apply(_, _))
  }

  /**
   * @return Returns this GenotypeRDD squared off as a VariantContextRDD.
   */
  def toVariantContextRDD: VariantContextRDD = {
    val vcIntRdd: RDD[(RichVariant, Genotype)] = rdd.keyBy(g => {
      RichVariant.genotypeToRichVariant(g)
    })
    val vcRdd = vcIntRdd.groupByKey
      .map {
        case (v: RichVariant, g) => {
          new VariantContext(ReferencePosition(v.variant), v, g)
        }
      }

    VariantContextRDD(vcRdd, sequences, samples, headerLines)
  }

  /**
   * Returns all reference regions that overlap this genotype.
   *
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new GenotypeRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Genotype],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): GenotypeRDD = {
    copy(rdd = newRdd, optPartitionMap = newPartitionMap)
  }

  /**
   * @param elem The genotype to get a reference region for.
   * @param stranded Whether or not to report stranded data for each Genotype
   *   such that true reports stranded data and false does not.
   * @return Returns the singular region this genotype covers.
   */
  protected def getReferenceRegions(elem: Genotype,
                                    stranded: Boolean = false): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
