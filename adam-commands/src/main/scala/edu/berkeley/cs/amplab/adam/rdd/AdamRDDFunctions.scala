/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.util.ContextUtil
import org.apache.avro.specific.SpecificRecord
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord, ADAMVariant, ADAMGenotype, ADAMVariantDomain}
import edu.berkeley.cs.amplab.adam.commands.ParquetArgs
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary, SingleReadBucket, ReferencePosition, ADAMRod}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import java.io.File
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext._
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.projections.{ADAMVariantAnnotations, ADAMVariantField}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

class AdamRDDFunctions[T <% SpecificRecord : Manifest](rdd: RDD[T]) extends Serializable {

  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false): RDD[T] = {
    val job = new Job(rdd.context.hadoopConfiguration)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].erasure.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].erasure.asInstanceOf[Class[T]], classOf[ParquetOutputFormat[T]],
      ContextUtil.getConfiguration(job))
    // Return the origin rdd
    rdd
  }

  def adamSave(filePath: String, parquetArgs: ParquetArgs): RDD[T] = {
    adamSave(filePath, parquetArgs.blockSize, parquetArgs.pageSize,
      parquetArgs.compressionCodec, parquetArgs.disableDictionary)
  }

}

class AdamRecordRDDFunctions(rdd: RDD[ADAMRecord]) extends Serializable with Logging {
  initLogging()

  def adamSortReadsByReferencePosition(): RDD[ADAMRecord] = {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we place them in a range of referenceIds at the end of the file.
    // The referenceId is an Int and typical only a few dozen values are even used.
    // These referenceId values are not stored; they are only used during sorting.
    val unmappedReferenceIds = new Iterator[Int] with Serializable {
      var currentOffsetFromEnd = 0

      def hasNext: Boolean = true

      def next(): Int = {
        currentOffsetFromEnd += 1
        if (currentOffsetFromEnd > 10000) {
          currentOffsetFromEnd = 0
        }
        Int.MaxValue - currentOffsetFromEnd
      }
    }

    rdd.map(p => {
      val referencePos = ReferencePosition(p) match {
        case None =>
          // Move unmapped reads to the end of the file
          ReferencePosition(unmappedReferenceIds.next(), Long.MaxValue)
        case Some(pos) => pos
      }
      (referencePos, p)
    }).sortByKey().map(p => p._2)
  }

  def sequenceDictionary() : SequenceDictionary =
    rdd.distinct().aggregate(SequenceDictionary())(
      (dict : SequenceDictionary, rec : ADAMRecord) => dict ++ SequenceRecord.fromADAMRecord(rec),
      (dict1 : SequenceDictionary, dict2 : SequenceDictionary) => dict1 ++ dict2)

  def adamMarkDuplicates(): RDD[ADAMRecord] = {
    MarkDuplicates(rdd)
  }

  def adamBQSR(dbSNP: File): RDD[ADAMRecord] = {
    val dbsnpMap = scala.io.Source.fromFile(dbSNP).getLines().map(posLine => {
      val split = posLine.split("\t")
      val contig = split(0)
      val pos = split(1).toInt
      (contig, pos)
    }).foldLeft(Map[String, Set[Int]]())((dbMap, pair) => {
      dbMap + (pair._1 -> (dbMap.getOrElse(pair._1, Set[Int]()) + pair._2))
    })

    val broadcastDbSNP = rdd.context.broadcast(dbsnpMap)

    RecalibrateBaseQualities(rdd, broadcastDbSNP)
  }

  // Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
  def adamFlagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by record group and read name
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  def adamSingleReadBuckets(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }

  /**
   * Groups all reads by reference position and returns a non-aggregated pileup RDD.
   * @return ADAMPileup without aggregation
   */
  def adamRecords2Pileup(): RDD[ADAMPileup] = {
    val helper = new Reads2PileupProcessor
    helper.process(rdd)
  }
}

class AdamPileupRDDFunctions(rdd: RDD[ADAMPileup]) extends Serializable with Logging {
  initLogging()

  /**
   * Aggregates pileup bases together.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with aggregated bases.
   *
   * @see AdamRodRDDFunctions#adamAggregateRods
   */
  def adamAggregatePileups(coverage: Int = 30): RDD[ADAMPileup] = {
    val helper = new PileupAggregator
    helper.aggregate(rdd, coverage)
  }

  /**
   * Converts ungrouped pileup bases into reference grouped bases.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with rods grouped by reference position.
   */
  def adamPileupsToRods(coverage: Int = 30): RDD[ADAMRod] = {
    val groups = rdd.groupBy((p: ADAMPileup) => p.getPosition, coverage)
    
    groups.map(kv => ADAMRod(kv._1, kv._2.toList))
  }

}

class AdamRodRDDFunctions(rdd: RDD[ADAMRod]) extends Serializable with Logging {
  initLogging()

  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns a flat RDD.
   *
   * @return Rods split up by samples and _not_ grouped together.
   */
  def adamSplitRodsBySamples(): RDD[ADAMRod] = {
    rdd.flatMap(_.splitBySamples)
  }

  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns an RDD where the samples are grouped by the reference position.
   *
   * @return Rods split up by samples and grouped together by position.
   */
  def adamDivideRodsBySamples(): RDD[(Long, List[ADAMRod])] = {
    rdd.keyBy(_.position).map(r => (r._1, r._2.splitBySamples))
  }

  /**
   * Inside of a rod, aggregates pileup bases together.
   * 
   * @return RDD with aggregated rods.
   *
   * @see AdamPileupRDDFunctions#adamAggregatePileups
   */
  def adamAggregateRods(): RDD[ADAMRod] = {
    val helper = new PileupAggregator
    rdd.map(r => (r.position, r.pileups))
      .map(kv => (kv._1, helper.flatten(kv._2)))
      .map(kv => new ADAMRod(kv._1, kv._2))
  }

  /**
   * Returns the average coverage for all pileups.
   *
   * @note Coverage value does not include locus positions where no reads are mapped, as no rods exist for these positions.
   * @note If running on an RDD with multiple samples where the rods have been split by sample, will return the average
   * coverage per sample, _averaged_ over all samples. If the RDD contains multiple samples and the rods have _not_ been split,
   * this will return the average coverage per sample, _summed_ over all samples.
   *
   * @return Average coverage across mapped loci.
   */
  def adamRodCoverage(): Double = {
    val totalBases: Long = rdd.map(_.pileups.length.toLong).reduce(_ + _)
    
    // coverage is the total count of bases, over the total number of loci
    totalBases.toDouble / rdd.count.toDouble
  }
}

class AdamVariantContextRDDFunctions(rdd: RDD[ADAMVariantContext]) extends Serializable {

  /**
   * Save function for variant contexts. Disaggregates internal fields of variant context
   * and saves to Parquet files.
   *
   * @param filePath Master file path for parquet files.
   * @param blockSize Parquet block size.
   * @param pageSize Parquet page size.
   * @param compressCodec Parquet compression codec.
   * @param disableDictionaryEncoding If true, disables dictionary encoding in Parquet.
   * @return Returns the initial RDD.
   */
  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false): RDD[ADAMVariantContext] = {

    // Add the Void Key
    val variantToSave: RDD[ADAMVariant] = rdd.flatMap(p => p.variants)
    val genotypeToSave: RDD[ADAMGenotype] = rdd.flatMap(p => p.genotypes)
    val domainsToSave: RDD[ADAMVariantDomain] = rdd.flatMap(p => p.domains)

    // save records
    variantToSave.adamSave(filePath + ".v", 
                           blockSize, 
                           pageSize, 
                           compressCodec, 
                           disableDictionaryEncoding)
    genotypeToSave.adamSave(filePath + ".g", 
                            blockSize, 
                            pageSize, 
                            compressCodec, 
                            disableDictionaryEncoding)
    
    // check if we have domains to save or not
    if (domainsToSave.count() != 0) {
      val fileExtension = ADAMVariantAnnotations.fileExtensions(ADAMVariantAnnotations.ADAMVariantDomain)

      domainsToSave.adamSave(filePath + fileExtension,
                             blockSize, 
                             pageSize, 
                             compressCodec, 
                             disableDictionaryEncoding)
    }

    rdd
  }

  /**
   * Save function for variant contexts. Disaggregates internal fields of variant context
   * and saves to Parquet files.
   *
   * @param filePath Master file path for parquet files.
   * @param parquetArgs Argument object for parquet.
   * @return Returns the initial RDD.
   */
  def adamSave(filePath: String, parquetArgs: ParquetArgs): RDD[ADAMVariantContext] = {
    adamSave(filePath, parquetArgs.blockSize, parquetArgs.pageSize,
      parquetArgs.compressionCodec, parquetArgs.disableDictionary)
  }
}

class AdamGenotypeRDDFunctions(rdd: RDD[ADAMGenotype]) extends Serializable {

  /**
   * Validates that an RDD of genotypes is correctly formed.
   *
   * @return True if RDD is correctly formed.
   * @throws IllegalArgumentException Throws exception if RDD is not correctly formed.
   */
  def adamValidateGenotypes(): Boolean = {
    val validator = new GenotypesToVariantsConverter(true, true)
    val groupedGenotypes = rdd.groupBy(g => (g.getPosition, g.getSampleId))
    groupedGenotypes.map(_._2.toList).foreach (validator.validateGenotypes)

    true
  }

  /**
   * Calculates Variants from an RDD of genotypes. This allows for on-the-fly creation of variant
   * data from a subset of a population. This function also allows an RDD of variant data to be provided.
   * Data can be taken from this RDD by adding to the projection set.
   *
   * @param variants Optional RDD of variant data to supplement genotype info.
   * @param variantProjection The set of fields to copy from the variant data, if this is provided.
   * @param performValidation Whether to validate that the genotype data is well formed.
   * @param failOnValidationError If validation is performed and failOnValidationError is true, an exception will
   * be thrown if an error is encountered.
   * @return An RDD containing variant data.
   *
   * @throws IllegalArgumentException Throws an exception if performValidation and failOnValidationError are true
   * and the RDD of genotypes has bad data.
   */
  def adamConvertGenotypes(variants: Option[RDD[ADAMVariant]] = None,
                           variantProjection: Set[ADAMVariantField.Value] = Set[ADAMVariantField.Value](),
                           performValidation: Boolean = false,
                           failOnValidationError: Boolean = false): RDD[ADAMVariant] = {
    val computer = new GenotypesToVariantsConverter(performValidation, failOnValidationError)
    val groupedGenotypes = rdd.groupBy(g => g.getPosition)
    val groupedGenotypesWithVariants: RDD[(java.lang.Long, (Seq[ADAMGenotype], Option[Seq[ADAMVariant]]))] = variants match {
      case Some(o) => groupedGenotypes.leftOuterJoin(o.asInstanceOf[RDD[ADAMVariant]].groupBy(_.getPosition))
      case None => groupedGenotypes.map(kv => (kv._1, (kv._2, None.asInstanceOf[Option[Seq[ADAMVariant]]])))
    }

    groupedGenotypesWithVariants.map(_._2).flatMap (vg => computer.convert(vg._1, vg._2, variantProjection))
  }
}
