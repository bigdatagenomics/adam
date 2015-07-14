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

import java.io.StringWriter

import htsjdk.samtools.{ SAMFileHeader, SAMTextHeaderCodec, SAMTextWriter, ValidationStringency }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.LongWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.algorithms.consensus.{ ConsensusGenerator, ConsensusGeneratorFromReads }
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.realignment.RealignIndels
import org.bdgenomics.adam.rdd.read.recalibration.BaseQualityRecalibration
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, ADAMSequenceDictionaryRDDAggregator }
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.MapTools
import org.bdgenomics.formats.avro._
import org.seqdoop.hadoop_bam.SAMRecordWritable

class AlignmentRecordRDDFunctions(rdd: RDD[AlignmentRecord])
    extends ADAMSequenceDictionaryRDDAggregator[AlignmentRecord](rdd) {

  /**
   * Calculates the subset of the RDD whose AlignmentRecords overlap the corresponding
   * query ReferenceRegion.  Equality of the reference sequence (to which these are aligned)
   * is tested by string equality of the names.  AlignmentRecords whose 'getReadMapped' method
   * return 'false' are ignored.
   *
   * The end of the record against the reference sequence is calculated from the cigar string
   * using the ADAMContext.referenceLengthFromCigar method.
   *
   * @param query The query region, only records which overlap this region are returned.
   * @return The subset of AlignmentRecords (corresponding to either primary or secondary alignments) that
   *         overlap the query region.
   */
  def filterByOverlappingRegion(query: ReferenceRegion): RDD[AlignmentRecord] = {
    def overlapsQuery(rec: AlignmentRecord): Boolean =
      rec.getReadMapped &&
        rec.getContig.getContigName.toString == query.referenceName &&
        rec.getStart < query.end &&
        rec.getEnd > query.start
    rdd.filter(overlapsQuery)
  }

  def maybeSaveBam(args: ADAMSaveAnyArgs): Boolean = {
    if (args.outputPath.endsWith(".sam")) {
      log.info("Saving data in SAM format")
      rdd.adamSAMSave(args.outputPath, asSingleFile = args.asSingleFile)
      true
    } else if (args.outputPath.endsWith(".bam")) {
      log.info("Saving data in BAM format")
      rdd.adamSAMSave(args.outputPath, asSam = false, asSingleFile = args.asSingleFile)
      true
    } else
      false
  }

  def maybeSaveFastq(args: ADAMSaveAnyArgs): Boolean = {
    if (args.outputPath.endsWith(".fq") || args.outputPath.endsWith(".fastq") ||
      args.outputPath.endsWith(".ifq")) {
      rdd.adamSaveAsFastq(args.outputPath, sort = args.sortFastqOutput)
      true
    } else
      false
  }

  def adamAlignedRecordSave(args: ADAMSaveAnyArgs) = {
    maybeSaveBam(args) || { rdd.adamParquetSave(args); true }
  }

  def adamSave(args: ADAMSaveAnyArgs) = {
    maybeSaveBam(args) || maybeSaveFastq(args) || { rdd.adamParquetSave(args); true }
  }

  def adamSAMString: String = {
    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) = rdd.adamConvertToSAM()

    val records = convertRecords.coalesce(1, shuffle = true).collect()

    val samHeaderCodec = new SAMTextHeaderCodec
    samHeaderCodec.setValidationStringency(ValidationStringency.SILENT)

    val samStringWriter = new StringWriter()
    samHeaderCodec.encode(samStringWriter, header);

    val samWriter: SAMTextWriter = new SAMTextWriter(samStringWriter)
    //samWriter.writeHeader(stringHeaderWriter.toString)

    records.foreach(record => samWriter.writeAlignment(record.get))

    samStringWriter.toString
  }

  /**
   * Saves an RDD of ADAM read data into the SAM/BAM format.
   *
   * @param filePath Path to save files to.
   * @param asSam Selects whether to save as SAM or BAM. The default value is true (save in SAM format).
   */
  def adamSAMSave(filePath: String, asSam: Boolean = true, asSingleFile: Boolean = false) = SAMSave.time {

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) = rdd.adamConvertToSAM()

    // add keys to our records
    val withKey =
      if (asSingleFile) convertRecords.keyBy(v => new LongWritable(v.get.getAlignmentStart)).coalesce(1)
      else convertRecords.keyBy(v => new LongWritable(v.get.getAlignmentStart))

    val bcastHeader = rdd.context.broadcast(header)
    val mp = rdd.mapPartitionsWithIndex((idx, iter) => {
      log.info(s"Setting ${if (asSam) "SAM" else "BAM"} header for partition $idx")
      val header = bcastHeader.value
      synchronized {
        // perform map partition call to ensure that the SAM/BAM header is set on all
        // nodes in the cluster; see:
        // https://github.com/bigdatagenomics/adam/issues/353,
        // https://github.com/bigdatagenomics/adam/issues/676

        asSam match {
          case true =>
            ADAMSAMOutputFormat.clearHeader()
            ADAMSAMOutputFormat.addHeader(header)
            log.info(s"Set SAM header for partition $idx")
          case false =>
            ADAMBAMOutputFormat.clearHeader()
            ADAMBAMOutputFormat.addHeader(header)
            log.info(s"Set BAM header for partition $idx")
        }
      }
      Iterator[Int]()
    }).count()

    // force value check, ensure that computation happens
    if (mp != 0) {
      log.error("Had more than 0 elements after map partitions call to set VCF header across cluster.")
    }

    // attach header to output format
    asSam match {
      case true =>
        ADAMSAMOutputFormat.clearHeader()
        ADAMSAMOutputFormat.addHeader(header)
        log.info(s"Set SAM header on driver")
      case false =>
        ADAMBAMOutputFormat.clearHeader()
        ADAMBAMOutputFormat.addHeader(header)
        log.info(s"Set BAM header on driver")
    }

    // write file to disk
    val conf = rdd.context.hadoopConfiguration
    asSam match {
      case true =>
        withKey.saveAsNewAPIHadoopFile(
          filePath,
          classOf[LongWritable],
          classOf[SAMRecordWritable],
          classOf[InstrumentedADAMSAMOutputFormat[LongWritable]],
          conf
        )
      case false =>
        withKey.saveAsNewAPIHadoopFile(
          filePath,
          classOf[LongWritable],
          classOf[SAMRecordWritable],
          classOf[InstrumentedADAMBAMOutputFormat[LongWritable]],
          conf
        )
    }
    if (asSingleFile) {
      log.info(s"Writing single ${if (asSam) "SAM" else "BAM"} file (not Hadoop-style directory)")
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val ouputParentDir = filePath.substring(0, filePath.lastIndexOf("/") + 1)
      val tmpPath = ouputParentDir + "tmp" + System.currentTimeMillis().toString
      fs.rename(new Path(filePath + "/part-r-00000"), new Path(tmpPath))
      fs.delete(new Path(filePath), true)
      fs.rename(new Path(tmpPath), new Path(filePath))
    }
  }

  def getSequenceRecordsFromElement(elem: AlignmentRecord): scala.collection.Set[SequenceRecord] = {
    SequenceRecord.fromADAMRecord(elem)
  }

  /**
   * Collects a dictionary summarizing the read groups in an RDD of ADAMRecords.
   *
   * @return A dictionary describing the read groups in this RDD.
   */
  def adamGetReadGroupDictionary(): RecordGroupDictionary = {
    val rgNames = rdd.flatMap(RecordGroup(_))
      .distinct()
      .collect()
      .toSeq

    new RecordGroupDictionary(rgNames)
  }

  /**
   * Converts an RDD of ADAM read records into SAM records.
   *
   * @return Returns a SAM/BAM formatted RDD of reads, as well as the file header.
   */
  def adamConvertToSAM(): (RDD[SAMRecordWritable], SAMFileHeader) = ConvertToSAM.time {
    // collect global summary data
    val sd = rdd.adamGetSequenceDictionary()
    val rgd = rdd.adamGetReadGroupDictionary()

    // create conversion object
    val adamRecordConverter = new AlignmentRecordConverter

    // create header
    val header = adamRecordConverter.createSAMHeader(sd, rgd)

    // broadcast for efficiency
    val hdrBcast = rdd.context.broadcast(SAMFileHeaderWritable(header))

    // map across RDD to perform conversion
    val convertedRDD: RDD[SAMRecordWritable] = rdd.map(r => {
      // must wrap record for serializability
      val srw = new SAMRecordWritable()
      srw.set(adamRecordConverter.convert(r, hdrBcast.value))
      srw
    })

    (convertedRDD, header)
  }

  /**
   * Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns an RDD containing k-mer/count pairs.
   *
   * @see adamCountQmers
   */
  def adamCountKmers(kmerLength: Int): RDD[(String, Long)] = {
    rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .toString
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  def adamSortReadsByReferencePosition(): RDD[AlignmentRecord] = SortReads.time {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. we prefix with "ZZZ" to ensure
    // that the read name is lexicographically "after" the contig names
    rdd.keyBy(r => {
      if (r.getReadMapped) {
        ReferencePosition(r)
      } else {
        ReferencePosition("ZZZ%s".format(r.getReadName), 0)
      }
    }).sortByKey().map(p => p._2)
  }

  def adamMarkDuplicates(): RDD[AlignmentRecord] = MarkDuplicatesInDriver.time {
    MarkDuplicates(rdd)
  }

  /**
   * Runs base quality score recalibration on a set of reads. Uses a table of
   * known SNPs to mask true variation during the recalibration process.
   *
   * @param knownSnps A table of known SNPs to mask valid variants.
   * @param observationDumpFile An optional local path to dump recalibration
   *                            observations to.
   * @return Returns an RDD of recalibrated reads.
   */
  def adamBQSR(knownSnps: Broadcast[SnpTable],
               observationDumpFile: Option[String] = None): RDD[AlignmentRecord] = BQSRInDriver.time {
    BaseQualityRecalibration(rdd, knownSnps, observationDumpFile)
  }

  /**
   * Realigns indels using a concensus-based heuristic.
   *
   * @see RealignIndels
   *
   * @param isSorted If the input data is sorted, setting this parameter to true avoids a second sort.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   * @param maxConsensusNumber The maximum number of consensus sequences to realign against per
   * target region.
   * @param lodThreshold Log-odds threhold to use when realigning; realignments are only finalized
   * if the log-odds threshold is exceeded.
   * @param maxTargetSize The maximum width of a single target region for realignment.
   *
   * @return Returns an RDD of mapped reads which have been realigned.
   */
  def adamRealignIndels(consensusModel: ConsensusGenerator = new ConsensusGeneratorFromReads,
                        isSorted: Boolean = false,
                        maxIndelSize: Int = 500,
                        maxConsensusNumber: Int = 30,
                        lodThreshold: Double = 5.0,
                        maxTargetSize: Int = 3000): RDD[AlignmentRecord] = RealignIndelsInDriver.time {
    RealignIndels(rdd, consensusModel, isSorted, maxIndelSize, maxConsensusNumber, lodThreshold)
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
   * Converts a set of records into an RDD containing the pairs of all unique tagStrings
   * within the records, along with the count (number of records) which have that particular
   * attribute.
   *
   * @return An RDD of attribute name / count pairs.
   */
  def adamCharacterizeTags(): RDD[(String, Long)] = {
    rdd.flatMap(RichAlignmentRecord(_).tags.map(attr => (attr.tag, 1L))).reduceByKey(_ + _)
  }

  /**
   * Calculates the set of unique attribute <i>values</i> that occur for the given
   * tag, and the number of time each value occurs.
   *
   * @param tag The name of the optional field whose values are to be counted.
   * @return A Map whose keys are the values of the tag, and whose values are the number of time each tag-value occurs.
   */
  def adamCharacterizeTagValues(tag: String): Map[Any, Long] = {
    adamFilterRecordsWithTag(tag).flatMap(RichAlignmentRecord(_).tags.find(_.tag == tag)).map(
      attr => Map(attr.value -> 1L)).reduce {
        (map1: Map[Any, Long], map2: Map[Any, Long]) =>
          MapTools.add(map1, map2)
      }
  }

  /**
   * Returns the subset of the ADAMRecords which have an attribute with the given name.
   * @param tagName The name of the attribute to filter on (should be length 2)
   * @return An RDD[Read] containing the subset of records with a tag that matches the given name.
   */
  def adamFilterRecordsWithTag(tagName: String): RDD[AlignmentRecord] = {
    assert(tagName.length == 2,
      "withAttribute takes a tagName argument of length 2; tagName=\"%s\"".format(tagName))
    rdd.filter(RichAlignmentRecord(_).tags.exists(_.tag == tagName))
  }

  /**
   * Saves these AlignmentRecords to two FASTQ files: one for the first mate in each pair, and the other for the second.
   *
   * @param fileName1 Path at which to save a FASTQ file containing the first mate of each pair.
   * @param fileName2 Path at which to save a FASTQ file containing the second mate of each pair.
   * @param validationStringency Iff strict, throw an exception if any read in this RDD is not accompanied by its mate.
   */
  def adamSaveAsPairedFastq(fileName1: String,
                            fileName2: String,
                            validationStringency: ValidationStringency = ValidationStringency.LENIENT,
                            persistLevel: Option[StorageLevel] = None): Unit = {
    def maybePersist[T](r: RDD[T]): Unit = {
      persistLevel.foreach(r.persist(_))
    }
    def maybeUnpersist[T](r: RDD[T]): Unit = {
      persistLevel.foreach(_ => r.unpersist())
    }

    maybePersist(rdd)
    val numRecords = rdd.count()

    val readsByID: RDD[(String, Iterable[AlignmentRecord])] =
      rdd.groupBy(record => {
        if (!AlignmentRecordConverter.readNameHasPairedSuffix(record))
          record.getReadName.toString
        else
          record.getReadName.toString.dropRight(2)
      })

    if (validationStringency == ValidationStringency.STRICT) {
      val readIDsWithCounts: RDD[(String, Int)] = readsByID.mapValues(_.size)
      val unpairedReadIDsWithCounts: RDD[(String, Int)] = readIDsWithCounts.filter(_._2 != 2)
      maybePersist(unpairedReadIDsWithCounts)

      val numUnpairedReadIDsWithCounts: Long = unpairedReadIDsWithCounts.count()
      if (numUnpairedReadIDsWithCounts != 0) {
        val readNameOccurrencesMap: collection.Map[Int, Long] = unpairedReadIDsWithCounts.map(_._2).countByValue()
        throw new Exception(
          "Found %d read names that don't occur exactly twice:\n%s\n\nSamples:\n%s".format(
            numUnpairedReadIDsWithCounts,
            readNameOccurrencesMap.map(p => "%dx:\t%d".format(p._1, p._2)).mkString("\t", "\n\t", ""),
            unpairedReadIDsWithCounts.take(100).map(_._1).mkString("\t", "\n\t", "")
          )
        )
      }
    }

    val pairedRecords: RDD[AlignmentRecord] = readsByID.filter(_._2.size == 2).map(_._2).flatMap(x => x)
    maybePersist(pairedRecords)
    val numPairedRecords = pairedRecords.count()

    maybeUnpersist(rdd.unpersist())

    val firstInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getFirstOfPair)
    maybePersist(firstInPairRecords)
    val numFirstInPairRecords = firstInPairRecords.count()

    val secondInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getSecondOfPair)
    maybePersist(secondInPairRecords)
    val numSecondInPairRecords = secondInPairRecords.count()

    maybeUnpersist(pairedRecords)

    log.info(
      "%d/%d records are properly paired: %d firsts, %d seconds".format(
        numPairedRecords,
        numRecords,
        numFirstInPairRecords,
        numSecondInPairRecords
      )
    )

    if (validationStringency == ValidationStringency.STRICT) {
      firstInPairRecords.foreach(read =>
        if (read.getSecondOfPair)
          throw new Exception("Read %s found with first- and second-of-pair set".format(read.getReadName))
      )
      secondInPairRecords.foreach(read =>
        if (read.getFirstOfPair)
          throw new Exception("Read %s found with first- and second-of-pair set".format(read.getReadName))
      )
    }

    assert(
      numFirstInPairRecords == numSecondInPairRecords,
      "Different numbers of first- and second-reads: %d vs. %d".format(numFirstInPairRecords, numSecondInPairRecords)
    )

    val arc = new AlignmentRecordConverter

    firstInPairRecords
      .sortBy(_.getReadName.toString)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true))
      .saveAsTextFile(fileName1)

    secondInPairRecords
      .sortBy(_.getReadName.toString)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true))
      .saveAsTextFile(fileName2)

    maybeUnpersist(firstInPairRecords)
    maybeUnpersist(secondInPairRecords)
  }

  /**
   * Saves reads in FASTQ format.
   *
   * @param fileName Path to save files at.
   * @param sort Whether to sort the FASTQ files by read name or not. Defaults
   *             to false. Sorting the output will recover pair order, if desired.
   */
  def adamSaveAsFastq(fileName: String,
                      fileName2Opt: Option[String] = None,
                      sort: Boolean = false,
                      validationStringency: ValidationStringency = ValidationStringency.LENIENT,
                      persistLevel: Option[StorageLevel] = None) {
    log.info("Saving data in FASTQ format.")
    fileName2Opt match {
      case Some(fileName2) =>
        adamSaveAsPairedFastq(
          fileName,
          fileName2,
          validationStringency = validationStringency,
          persistLevel = persistLevel
        )
      case _ =>
        val arc = new AlignmentRecordConverter

        // sort the rdd if desired
        val outputRdd = if (sort || fileName2Opt.isDefined) {
          rdd.sortBy(_.getReadName.toString)
        } else {
          rdd
        }

        // convert the rdd and save as a text file
        outputRdd
          .map(record => arc.convertToFastq(record))
          .saveAsTextFile(fileName)
    }
  }

  /**
   * Reassembles read pairs from two sets of unpaired reads. The assumption is that the two sets
   * were _originally_ paired together.
   *
   * @note The RDD that this is called on should be the RDD with the first read from the pair.
   *
   * @param secondPairRdd The rdd containing the second read from the pairs.
   * @param validationStringency How stringently to validate the reads.
   * @return Returns an RDD with the pair information recomputed.
   */
  def adamRePairReads(secondPairRdd: RDD[AlignmentRecord],
                      validationStringency: ValidationStringency = ValidationStringency.LENIENT): RDD[AlignmentRecord] = {
    // cache rdds
    val firstPairRdd = rdd.cache()
    secondPairRdd.cache()

    val firstRDDKeyedByReadName = firstPairRdd.keyBy(_.getReadName.toString.dropRight(2))
    val secondRDDKeyedByReadName = secondPairRdd.keyBy(_.getReadName.toString.dropRight(2))

    // all paired end reads should have the same name, except for the last two
    // characters, which will be _1/_2
    val joinedRDD: RDD[(String, (AlignmentRecord, AlignmentRecord))] =
      if (validationStringency == ValidationStringency.STRICT) {
        firstRDDKeyedByReadName.cogroup(secondRDDKeyedByReadName).map {
          case (readName, (firstReads, secondReads)) =>
            (firstReads.toList, secondReads.toList) match {
              case (firstRead :: Nil, secondRead :: Nil) =>
                (readName, (firstRead, secondRead))
              case _ =>
                throw new Exception(
                  "Expected %d first reads and %d second reads for name %s; expected exactly one of each:\n%s\n%s".format(
                    firstReads.size,
                    secondReads.size,
                    readName,
                    firstReads.map(_.getReadName.toString).mkString("\t", "\n\t", ""),
                    secondReads.map(_.getReadName.toString).mkString("\t", "\n\t", "")
                  )
                )
            }
        }

      } else {
        firstRDDKeyedByReadName.join(secondRDDKeyedByReadName)
      }

    val finalRdd = joinedRDD
      .flatMap(kv => Seq(
        AlignmentRecord.newBuilder(kv._2._1)
          .setReadPaired(true)
          .setProperPair(true)
          .setFirstOfPair(true)
          .setSecondOfPair(false)
          .build(),
        AlignmentRecord.newBuilder(kv._2._2)
          .setReadPaired(true)
          .setProperPair(true)
          .setFirstOfPair(false)
          .setSecondOfPair(true)
          .build()
      ))

    // uncache temp rdds
    firstPairRdd.unpersist()
    secondPairRdd.unpersist()

    // return
    finalRdd
  }
}
