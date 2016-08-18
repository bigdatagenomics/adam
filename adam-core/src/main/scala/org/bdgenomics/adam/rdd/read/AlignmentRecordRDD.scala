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

import htsjdk.samtools._
import htsjdk.samtools.util.{
  BinaryCodec,
  BlockCompressedOutputStream,
  BlockCompressedStreamConstants
}
import java.io.{ InputStream, OutputStream, StringWriter, Writer }
import htsjdk.samtools._
import htsjdk.samtools.util.{ BinaryCodec, BlockCompressedOutputStream }
import org.apache.avro.Schema
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.LongWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.algorithms.consensus.{
  ConsensusGenerator,
  ConsensusGeneratorFromReads
}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  AvroReadGroupGenomicRDD,
  ADAMSaveAnyArgs,
  JavaSaveArgs,
  Unaligned
}
import org.bdgenomics.adam.rdd.features.CoverageRDD
import org.bdgenomics.adam.rdd.read.realignment.RealignIndels
import org.bdgenomics.adam.rdd.read.recalibration.BaseQualityRecalibration
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.MapTools
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.Logging
import org.seqdoop.hadoop_bam.{ SAMFormat, SAMRecordWritable }
import org.seqdoop.hadoop_bam.util.SAMOutputPreparer
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.math.{ abs, min }

sealed trait AlignmentRecordRDD extends AvroReadGroupGenomicRDD[AlignmentRecord, AlignmentRecordRDD] {

  /**
   * Replaces the underlying RDD and SequenceDictionary and emits a new object.
   *
   * @param newRdd New RDD to replace current RDD.
   * @param newSequences New sequence dictionary to replace current dictionary.
   * @return Returns a new AlignmentRecordRDD.
   */
  protected def replaceRddAndSequences(newRdd: RDD[AlignmentRecord],
                                       newSequences: SequenceDictionary): AlignmentRecordRDD

  /**
   * Convert this set of reads into fragments.
   *
   * @return Returns a FragmentRDD where all reads have been grouped together by
   *   the original sequence fragment they come from.
   */
  def toFragments: FragmentRDD = {
    FragmentRDD(groupReadsByFragment().map(_.toFragment),
      sequences,
      recordGroups)
  }

  /**
   * Converts this set of reads into a corresponding CoverageRDD.
   *
   * @param collapse Determines whether to merge adjacent coverage elements with the same score a single coverage.
   * @return CoverageRDD containing mapped RDD of Coverage.
   */
  def toCoverage(collapse: Boolean = true): CoverageRDD = {
    val covCounts =
      rdd.rdd
        .flatMap(r => {
          val t: List[Long] = List.range(r.getStart, r.getEnd)
          t.map(n => (ReferenceRegion(r.getContigName, n, n + 1), 1))
        }).reduceByKey(_ + _)
        .cache()

    val coverage = (
      if (collapse) covCounts.sortByKey()
      else covCounts
    ).map(r => Coverage(r._1, r._2.toDouble))

    val coverageRdd =
      if (collapse) CoverageRDD(coverage, sequences)
        .collapse
      else CoverageRDD(coverage, sequences)

    covCounts.unpersist()
    coverageRdd
  }

  /**
   * Returns all reference regions that overlap this read.
   *
   * If a read is unaligned, it covers no reference region. If a read is aligned
   * we expect it to cover a single region. A chimeric read would cover multiple
   * regions, but we store chimeric reads in a way similar to BAM, where the
   * split alignments are stored in multiple separate reads.
   *
   * @param elem Read to produce regions for.
   * @return The seq of reference regions this read covers.
   */
  protected def getReferenceRegions(elem: AlignmentRecord): Seq[ReferenceRegion] = {
    ReferenceRegion.opt(elem).toSeq
  }

  /**
   * Saves this RDD as BAM or SAM if the extension provided is .sam or .bam.
   *
   * @param args Arguments defining where to save the file.
   * @param isSorted True if input data is sorted. Sets the ordering in the SAM
   *   file header.
   * @return Returns true if the extension in args ended in .sam/.bam and the
   *   file was saved.
   */
  private[rdd] def maybeSaveBam(args: ADAMSaveAnyArgs,
                                isSorted: Boolean = false): Boolean = {

    if (args.outputPath.endsWith(".sam")) {
      log.info("Saving data in SAM format")
      saveAsSam(
        args.outputPath,
        asSingleFile = args.asSingleFile,
        isSorted = isSorted
      )
      true
    } else if (args.outputPath.endsWith(".bam")) {
      log.info("Saving data in BAM format")
      saveAsSam(
        args.outputPath,
        asSam = false,
        asSingleFile = args.asSingleFile,
        isSorted = isSorted
      )
      true
    } else {
      false
    }
  }

  /**
   * Saves the RDD as FASTQ if the file has the proper extension.
   *
   * @param args Save arguments defining the file path to save at.
   * @return True if the file extension ended in ".fq" or ".fastq" and the file
   *   was saved as FASTQ, or if the file extension ended in ".ifq" and the file
   *   was saved as interleaved FASTQ.
   */
  private[rdd] def maybeSaveFastq(args: ADAMSaveAnyArgs): Boolean = {
    if (args.outputPath.endsWith(".fq") || args.outputPath.endsWith(".fastq") ||
      args.outputPath.endsWith(".ifq")) {
      saveAsFastq(args.outputPath, sort = args.sortFastqOutput)
      true
    } else
      false
  }

  /**
   * Saves AlignmentRecords as a directory of Parquet files or as SAM/BAM.
   *
   * This method infers the output format from the file extension. Filenames
   * ending in .sam/.bam are saved as SAM/BAM, and all other files are saved
   * as Parquet.
   *
   * @param args Save configuration arguments.
   * @param isSorted If the output is sorted, this will modify the SAM/BAM header.
   * @return Returns true if saving succeeded.
   */
  def save(args: ADAMSaveAnyArgs,
           isSorted: Boolean = false): Boolean = {

    (maybeSaveBam(args, isSorted) ||
      maybeSaveFastq(args) ||
      { saveAsParquet(args); true })
  }

  /**
   * Saves this RDD to disk, with the type identified by the extension.
   *
   * @param filePath Path to save the file at.
   * @param isSorted Whether the file is sorted or not.
   * @return Returns true if saving succeeded.
   */
  def save(filePath: java.lang.String,
           isSorted: java.lang.Boolean): java.lang.Boolean = {
    save(new JavaSaveArgs(filePath), isSorted)
  }

  /**
   * Converts an RDD into the SAM spec string it represents.
   *
   * This method converts an RDD of AlignmentRecords back to an RDD of
   * SAMRecordWritables and a SAMFileHeader, and then maps this RDD into a
   * string on the driver that represents this file in SAM.
   *
   * @return A string on the driver representing this RDD of reads in SAM format.
   */
  def saveAsSamString(): String = {

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) = convertToSam()

    // collect the records to the driver
    val records = convertRecords.collect()

    // get a header writing codec
    val samHeaderCodec = new SAMTextHeaderCodec
    samHeaderCodec.setValidationStringency(ValidationStringency.SILENT)

    // create a stringwriter and write the header to it
    val samStringWriter = new StringWriter()
    samHeaderCodec.encode(samStringWriter, header)

    // create a sam text writer
    val samWriter: SAMTextWriter = new SAMTextWriter(samStringWriter)

    // write all records to the writer
    records.foreach(record => samWriter.writeAlignment(record.get))

    // return the writer as a string
    samStringWriter.toString
  }

  /**
   * Converts an RDD of ADAM read records into SAM records.
   *
   * @return Returns a SAM/BAM formatted RDD of reads, as well as the file header.
   */
  def convertToSam(isSorted: Boolean = false): (RDD[SAMRecordWritable], SAMFileHeader) = ConvertToSAM.time {

    // create conversion object
    val adamRecordConverter = new AlignmentRecordConverter

    // create header and set sort order if needed
    val header = adamRecordConverter.createSAMHeader(sequences, recordGroups)
    if (isSorted) {
      header.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    }

    // broadcast for efficiency
    val hdrBcast = rdd.context.broadcast(SAMFileHeaderWritable(header))

    // map across RDD to perform conversion
    val convertedRDD: RDD[SAMRecordWritable] = rdd.map(r => {
      // must wrap record for serializability
      val srw = new SAMRecordWritable()
      srw.set(adamRecordConverter.convert(r, hdrBcast.value, recordGroups))
      srw
    })

    (convertedRDD, header)
  }

  /**
   * Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(kmerLength: Int): RDD[(String, Long)] = {
    rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  /**
   * Saves an RDD of ADAM read data into the SAM/BAM format.
   *
   * @param filePath Path to save files to.
   * @param asSam Selects whether to save as SAM or BAM. The default value is true (save in SAM format).
   * @param asSingleFile If true, saves output as a single file.
   * @param isSorted If the output is sorted, this will modify the header.
   */
  def saveAsSam(
    filePath: String,
    asSam: Boolean = true,
    asSingleFile: Boolean = false,
    isSorted: Boolean = false): Unit = SAMSave.time {

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) =
      convertToSam(isSorted)

    // add keys to our records
    val withKey = convertRecords.keyBy(v => new LongWritable(v.get.getAlignmentStart))

    // write file to disk
    val conf = rdd.context.hadoopConfiguration

    if (!asSingleFile) {
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
          log.info("Set SAM header on driver")
        case false =>
          ADAMBAMOutputFormat.clearHeader()
          ADAMBAMOutputFormat.addHeader(header)
          log.info("Set BAM header on driver")
      }

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
    } else {
      log.info(s"Writing single ${if (asSam) "SAM" else "BAM"} file (not Hadoop-style directory)")

      val headPath = new Path(filePath + "_head")
      val tailPath = new Path(filePath + "_tail")
      val outputPath = new Path(filePath)
      val fs = headPath.getFileSystem(rdd.context.hadoopConfiguration)

      // get an output stream
      val os = fs.create(headPath)
        .asInstanceOf[OutputStream]

      // TIL: sam and bam are written in completely different ways!
      if (asSam) {
        val sw: Writer = new StringWriter()
        val stw = new SAMTextWriter(os)
        stw.setHeader(header)
        stw.close()
      } else {
        // create htsjdk specific streams for writing the bam header
        val compressedOut: OutputStream = new BlockCompressedOutputStream(os, null)
        val binaryCodec = new BinaryCodec(compressedOut);

        // write a bam header - cribbed from Hadoop-BAM
        binaryCodec.writeBytes("BAM\001".getBytes())
        val sw: Writer = new StringWriter()
        new SAMTextHeaderCodec().encode(sw, header)
        binaryCodec.writeString(sw.toString, true, false)

        // write sequence dictionary
        val ssd = header.getSequenceDictionary
        binaryCodec.writeInt(ssd.size())
        ssd.getSequences
          .toList
          .foreach(r => {
            binaryCodec.writeString(r.getSequenceName(), true, true)
            binaryCodec.writeInt(r.getSequenceLength())
          })

        // flush and close all the streams
        compressedOut.flush()
        compressedOut.close()
      }

      os.flush()
      os.close()

      // set path to header file
      conf.set("org.bdgenomics.adam.rdd.read.bam_header_path", headPath.toString)

      // set up output format
      val headerLessOutputFormat = if (asSam) {
        classOf[InstrumentedADAMSAMOutputFormatHeaderLess[LongWritable]]
      } else {
        classOf[InstrumentedADAMBAMOutputFormatHeaderLess[LongWritable]]
      }

      // save rdd
      withKey.saveAsNewAPIHadoopFile(
        tailPath.toString,
        classOf[LongWritable],
        classOf[SAMRecordWritable],
        headerLessOutputFormat,
        conf
      )

      // get a list of all of the files in the tail file
      val tailFiles = fs.globStatus(new Path("%s/part-*".format(tailPath)))
        .toSeq
        .map(_.getPath)
        .sortBy(_.getName)
        .toArray

      // try to merge this via the fs api, which should guarantee ordering...?
      // however! this function is not implemented on all platforms, hence the try.
      try {

        // we need to move the head file into the tailFiles directory
        // this is a requirement of the concat method
        val newHeadPath = new Path("%s/header".format(tailPath))
        fs.rename(headPath, newHeadPath)

        try {
          fs.concat(newHeadPath, tailFiles)
        } catch {
          case t: Throwable => {
            // move the head file back - essentially, unroll the prep for concat
            fs.rename(newHeadPath, headPath)
            throw t
          }
        }

        // move concatenated file
        fs.rename(newHeadPath, outputPath)

        // delete tail files
        fs.delete(tailPath, true)
      } catch {
        case e: Throwable => {

          log.warn("Caught exception when merging via Hadoop FileSystem API:\n%s".format(e))
          log.warn("Retrying as manual copy from the driver which will degrade performance.")

          // doing this correctly is surprisingly hard
          // specifically, copy merge does not care about ordering, which is
          // fine if your files are unordered, but if the blocks in the file
          // _are_ ordered, then hahahahahahahahahaha. GOOD. TIMES.
          //
          // fortunately, the blocks in our file are ordered
          // the performance of this section is hilarious
          // 
          // specifically, the performance is hilariously bad
          //
          // but! it is correct.

          // open our output file
          val os = fs.create(outputPath)

          // prepare output
          val format = if (asSam) {
            SAMFormat.SAM
          } else {
            SAMFormat.BAM
          }
          new SAMOutputPreparer().prepareForRecords(os, format, header);

          // here is a byte array for copying
          val ba = new Array[Byte](1024)

          @tailrec def copy(is: InputStream,
                            los: OutputStream) {

            // make a read
            val bytesRead = is.read(ba)

            // did our read succeed? if so, write to output stream
            // and continue
            if (bytesRead >= 0) {
              los.write(ba, 0, bytesRead)

              copy(is, los)
            }
          }

          // loop over allllll the files and copy them
          val numFiles = tailFiles.length
          var filesCopied = 1
          tailFiles.toSeq.foreach(p => {

            // print a bit of progress logging
            log.info("Copying file %s, file %d of %d.".format(
              p.toString,
              filesCopied,
              numFiles))

            // open our input file
            val is = fs.open(p)

            // until we are out of bytes, copy
            copy(is, os)

            // close our input stream
            is.close()

            // increment file copy count
            filesCopied += 1
          })

          // finish the file off
          if (!asSam) {
            os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
          }

          // flush and close the output stream
          os.flush()
          os.close()

          // delete temp files
          fs.delete(headPath, true)
          fs.delete(tailPath, true)
        }
      }
    }
  }

  /**
   * Saves this RDD to disk as a SAM/BAM file.
   *
   * @param filePath Path to save the file at.
   * @param asSam If true, saves as SAM. If false, saves as BAM.
   * @param asSingleFile If true, saves output as a single file.
   * @param isSorted If the output is sorted, this will modify the header.
   */
  def saveAsSam(
    filePath: java.lang.String,
    asSam: java.lang.Boolean,
    asSingleFile: java.lang.Boolean,
    isSorted: java.lang.Boolean) {
    saveAsSam(filePath,
      asSam = asSam,
      asSingleFile = asSingleFile,
      isSorted = isSorted)
  }

  /**
   * Sorts our read data by reference positions, with contigs ordered by name.
   *
   * Sorts reads by the location where they are aligned. Unaligned reads are
   * put at the end and sorted by read name. Contigs are ordered
   * lexicographically.
   *
   * @return Returns a new RDD containing sorted reads.
   *
   * @see sortReadsByReferencePositionAndIndex
   */
  def sortReadsByReferencePosition(): AlignmentRecordRDD = SortReads.time {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. We prefix with tildes ("~";
    // ASCII 126) to ensure that the read name is lexicographically "after" the
    // contig names.
    replaceRddAndSequences(rdd.sortBy(r => {
      if (r.getReadMapped) {
        ReferencePosition(r)
      } else {
        ReferencePosition(s"~~~${r.getReadName}", 0)
      }
    }), sequences.stripIndices.sorted)
  }

  /**
   * Sorts our read data by reference positions, with contigs ordered by index.
   *
   * Sorts reads by the location where they are aligned. Unaligned reads are
   * put at the end and sorted by read name. Contigs are ordered by index
   * that they are ordered in the SequenceDictionary.
   *
   * @return Returns a new RDD containing sorted reads.
   *
   * @see sortReadsByReferencePosition
   */
  def sortReadsByReferencePositionAndIndex(): AlignmentRecordRDD = SortByIndex.time {
    log.info("Sorting reads by reference index, using %s.".format(sequences))

    import scala.math.Ordering.{ Int => ImplicitIntOrdering, _ }

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. To do this, we hash the sequence name
    // and add the max contig index
    val maxContigIndex = sequences.records.flatMap(_.referenceIndex).max
    replaceRdd(rdd.sortBy(r => {
      if (r.getReadMapped) {
        val sr = sequences(r.getContigName)
        require(sr.isDefined, "Read %s has contig name %s not in dictionary %s.".format(
          r, r.getContigName, sequences))
        require(sr.get.referenceIndex.isDefined,
          "Contig %s from sequence dictionary lacks an index.".format(sr))

        (sr.get.referenceIndex.get, r.getStart: Long)
      } else {
        (min(abs(r.getReadName.hashCode + maxContigIndex), Int.MaxValue), 0L)
      }
    }))
  }

  /**
   * Marks reads as possible fragment duplicates.
   *
   * @return A new RDD where reads have the duplicate read flag set. Duplicate
   *   reads are NOT filtered out.
   */
  def markDuplicates(): AlignmentRecordRDD = MarkDuplicatesInDriver.time {
    replaceRdd(MarkDuplicates(this))
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
  def recalibateBaseQualities(
    knownSnps: Broadcast[SnpTable],
    observationDumpFile: Option[String] = None,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT): AlignmentRecordRDD = BQSRInDriver.time {
    replaceRdd(BaseQualityRecalibration(rdd, knownSnps, observationDumpFile, validationStringency))
  }

  /**
   * Realigns indels using a concensus-based heuristic.
   *
   * @param consensusModel The model to use for generating consensus sequences
   *   to realign against.
   * @param isSorted If the input data is sorted, setting this parameter to true
   *   avoids a second sort.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   * @param maxConsensusNumber The maximum number of consensus sequences to
   *   realign against per target region.
   * @param lodThreshold Log-odds threshold to use when realigning; realignments
   *   are only finalized if the log-odds threshold is exceeded.
   * @param maxTargetSize The maximum width of a single target region for
   *   realignment.
   * @return Returns an RDD of mapped reads which have been realigned.
   */
  def realignIndels(
    consensusModel: ConsensusGenerator = new ConsensusGeneratorFromReads,
    isSorted: Boolean = false,
    maxIndelSize: Int = 500,
    maxConsensusNumber: Int = 30,
    lodThreshold: Double = 5.0,
    maxTargetSize: Int = 3000): AlignmentRecordRDD = RealignIndelsInDriver.time {
    replaceRdd(RealignIndels(rdd, consensusModel, isSorted, maxIndelSize, maxConsensusNumber, lodThreshold))
  }

  /**
   * Runs a quality control pass akin to the Samtools FlagStat tool.
   *
   * @return Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
   */
  def flagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by record group and read name.
   *
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  def groupReadsByFragment(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }

  /**
   * Saves these AlignmentRecords to two FASTQ files.
   *
   * The files are one for the first mate in each pair, and the other for the
   * second mate in the pair.
   *
   * @param fileName1 Path at which to save a FASTQ file containing the first
   *   mate of each pair.
   * @param fileName2 Path at which to save a FASTQ file containing the second
   *   mate of each pair.
   * @param outputOriginalBaseQualities If true, writes out reads with the base
   *   qualities from the original qualities (SAM "OQ") field. If false, writes
   *   out reads with the base qualities from the qual field. Default is false.
   * @param validationStringency Iff strict, throw an exception if any read in
   *   this RDD is not accompanied by its mate.
   * @param persistLevel An optional persistance level to set. If this level is
   *   set, then reads will be cached (at the given persistance) level between
   *   passes.
   */
  def saveAsPairedFastq(
    fileName1: String,
    fileName2: String,
    outputOriginalBaseQualities: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT,
    persistLevel: Option[StorageLevel] = None) {

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
          record.getReadName
        else
          record.getReadName.dropRight(2)
      })

    validationStringency match {
      case ValidationStringency.STRICT | ValidationStringency.LENIENT =>
        val readIDsWithCounts: RDD[(String, Int)] = readsByID.mapValues(_.size)
        val unpairedReadIDsWithCounts: RDD[(String, Int)] = readIDsWithCounts.filter(_._2 != 2)
        maybePersist(unpairedReadIDsWithCounts)

        val numUnpairedReadIDsWithCounts: Long = unpairedReadIDsWithCounts.count()
        if (numUnpairedReadIDsWithCounts != 0) {
          val readNameOccurrencesMap: collection.Map[Int, Long] = unpairedReadIDsWithCounts.map(_._2).countByValue()

          val msg =
            List(
              s"Found $numUnpairedReadIDsWithCounts read names that don't occur exactly twice:",

              readNameOccurrencesMap.take(100).map({
                case (numOccurrences, numReadNames) => s"${numOccurrences}x:\t$numReadNames"
              }).mkString("\t", "\n\t", if (readNameOccurrencesMap.size > 100) "\n\t…" else ""),
              "",

              "Samples:",
              unpairedReadIDsWithCounts
                .take(100)
                .map(_._1)
                .mkString("\t", "\n\t", if (numUnpairedReadIDsWithCounts > 100) "\n\t…" else "")
            ).mkString("\n")

          if (validationStringency == ValidationStringency.STRICT)
            throw new IllegalArgumentException(msg)
          else if (validationStringency == ValidationStringency.LENIENT)
            logError(msg)
        }
      case ValidationStringency.SILENT =>
    }

    val pairedRecords: RDD[AlignmentRecord] = readsByID.filter(_._2.size == 2).map(_._2).flatMap(x => x)
    maybePersist(pairedRecords)
    val numPairedRecords = pairedRecords.count()

    maybeUnpersist(rdd.unpersist())

    val firstInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getReadInFragment == 0)
    maybePersist(firstInPairRecords)
    val numFirstInPairRecords = firstInPairRecords.count()

    val secondInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getReadInFragment == 1)
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

    assert(
      numFirstInPairRecords == numSecondInPairRecords,
      "Different numbers of first- and second-reads: %d vs. %d".format(numFirstInPairRecords, numSecondInPairRecords)
    )

    val arc = new AlignmentRecordConverter

    firstInPairRecords
      .sortBy(_.getReadName)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true, outputOriginalBaseQualities = outputOriginalBaseQualities))
      .saveAsTextFile(fileName1)

    secondInPairRecords
      .sortBy(_.getReadName)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true, outputOriginalBaseQualities = outputOriginalBaseQualities))
      .saveAsTextFile(fileName2)

    maybeUnpersist(firstInPairRecords)
    maybeUnpersist(secondInPairRecords)
  }

  /**
   * Saves reads in FASTQ format.
   *
   * @param fileName Path to save files at.
   * @param fileName2Opt Optional second path for saving files. If set, two
   *   files will be saved.
   * @param outputOriginalBaseQualities If true, writes out reads with the base
   *   qualities from the original qualities (SAM "OQ") field. If false, writes
   *   out reads with the base qualities from the qual field. Default is false.
   * @param sort Whether to sort the FASTQ files by read name or not. Defaults
   *   to false. Sorting the output will recover pair order, if desired.
   * @param validationStringency Iff strict, throw an exception if any read in
   *   this RDD is not accompanied by its mate.
   * @param persistLevel An optional persistance level to set. If this level is
   *   set, then reads will be cached (at the given persistance) level between
   *   passes.
   */
  def saveAsFastq(
    fileName: String,
    fileName2Opt: Option[String] = None,
    outputOriginalBaseQualities: Boolean = false,
    sort: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT,
    persistLevel: Option[StorageLevel] = None) {
    log.info("Saving data in FASTQ format.")
    fileName2Opt match {
      case Some(fileName2) =>
        saveAsPairedFastq(
          fileName,
          fileName2,
          outputOriginalBaseQualities = outputOriginalBaseQualities,
          validationStringency = validationStringency,
          persistLevel = persistLevel
        )
      case _ =>
        val arc = new AlignmentRecordConverter

        // sort the rdd if desired
        val outputRdd = if (sort || fileName2Opt.isDefined) {
          rdd.sortBy(_.getReadName)
        } else {
          rdd
        }

        // convert the rdd and save as a text file
        outputRdd
          .map(record => arc.convertToFastq(record, outputOriginalBaseQualities = outputOriginalBaseQualities))
          .saveAsTextFile(fileName)
    }
  }

  /**
   * Reassembles read pairs from two sets of unpaired reads. The assumption is that the two sets
   * were _originally_ paired together.
   *
   * @note The RDD that this is called on should be the RDD with the first read from the pair.
   * @param secondPairRdd The rdd containing the second read from the pairs.
   * @param validationStringency How stringently to validate the reads.
   * @return Returns an RDD with the pair information recomputed.
   */
  def reassembleReadPairs(
    secondPairRdd: RDD[AlignmentRecord],
    validationStringency: ValidationStringency = ValidationStringency.LENIENT): AlignmentRecordRDD = {
    // cache rdds
    val firstPairRdd = rdd.cache()
    secondPairRdd.cache()

    val firstRDDKeyedByReadName = firstPairRdd.keyBy(_.getReadName.dropRight(2))
    val secondRDDKeyedByReadName = secondPairRdd.keyBy(_.getReadName.dropRight(2))

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
                    firstReads.map(_.getReadName).mkString("\t", "\n\t", ""),
                    secondReads.map(_.getReadName).mkString("\t", "\n\t", "")
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
          .setReadInFragment(0)
          .build(),
        AlignmentRecord.newBuilder(kv._2._2)
          .setReadPaired(true)
          .setProperPair(true)
          .setReadInFragment(1)
          .build()
      ))

    // uncache temp rdds
    firstPairRdd.unpersist()
    secondPairRdd.unpersist()

    // return
    replaceRdd(finalRdd)
  }
}

case class AlignedReadRDD(rdd: RDD[AlignmentRecord],
                          sequences: SequenceDictionary,
                          recordGroups: RecordGroupDictionary) extends AlignmentRecordRDD {

  protected def replaceRddAndSequences(newRdd: RDD[AlignmentRecord],
                                       newSequences: SequenceDictionary): AlignmentRecordRDD = {
    AlignedReadRDD(newRdd,
      newSequences,
      recordGroups)
  }

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

  protected def replaceRddAndSequences(newRdd: RDD[AlignmentRecord],
                                       newSequences: SequenceDictionary): AlignmentRecordRDD = {

    // are we still unaligned?
    if (newSequences.isEmpty) {
      UnalignedReadRDD(newRdd,
        recordGroups)
    } else {
      AlignedReadRDD(newRdd,
        newSequences,
        recordGroups)
    }
  }
}

