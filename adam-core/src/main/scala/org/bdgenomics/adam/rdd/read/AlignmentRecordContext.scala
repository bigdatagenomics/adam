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

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.FastqRecordConverter
import org.bdgenomics.adam.io.{
  InterleavedFastqInputFormat,
  SingleFastqInputFormat
}
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.formats.avro.AlignmentRecord
import parquet.hadoop.util.ContextUtil

object AlignmentRecordContext extends Serializable with Logging {
  // Add ADAM Spark context methods
  implicit def adamContextToADAMContext(ac: ADAMContext): AlignmentRecordContext = new AlignmentRecordContext(ac.sc)

  // Add methods specific to Read RDDs
  implicit def rddToADAMRecordRDD(rdd: RDD[AlignmentRecord]) = new AlignmentRecordRDDFunctions(rdd)

  private[rdd] def adamInterleavedFastqLoad(sc: SparkContext,
                                            filePath: String): RDD[AlignmentRecord] = {
    log.info("Reading interleaved FASTQ file format %s to create RDD".format(filePath))

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[InterleavedFastqInputFormat],
      classOf[Void],
      classOf[Text],
      ContextUtil.getConfiguration(job)
    )
    val fastqRecordConverter = new FastqRecordConverter
    records.flatMap(fastqRecordConverter.convertPair)
  }

  private[rdd] def adamUnpairedFastqLoad(sc: SparkContext,
                                         filePath: String): RDD[AlignmentRecord] = {
    log.info("Reading unpaired FASTQ file format %s to create RDD".format(filePath))

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text],
      ContextUtil.getConfiguration(job)
    )
    val fastqRecordConverter = new FastqRecordConverter
    records.map(fastqRecordConverter.convertRead)
  }
}

class AlignmentRecordContext(val sc: SparkContext) extends Serializable with Logging {

  /**
   * Load AlignmentRecords from two paired-end FASTQ files.
   *
   * @param firstPairPath Path to read first-mates from
   * @param secondPairPath Path to read second-mates from
   * @param fixPairs Iff true, joins the first- and second-reads around their read name (minus the /1 or /2 suffix)
   */
  def adamFastqLoad(firstPairPath: String,
                    secondPairPath: String,
                    fixPairs: Boolean = false,
                    validationStringency: ValidationStringency = ValidationStringency.LENIENT): RDD[AlignmentRecord] = {
    // load rdds
    val firstPairRdd = AlignmentRecordContext.adamUnpairedFastqLoad(sc, firstPairPath)
    val secondPairRdd = AlignmentRecordContext.adamUnpairedFastqLoad(sc, secondPairPath)

    // cache rdds
    firstPairRdd.cache()
    secondPairRdd.cache()

    // we can simply process these IFF each has the same number of reads and
    // the user hasn't asked us to fix the pairs
    val finalRdd: RDD[AlignmentRecord] = if (firstPairRdd.count == secondPairRdd.count && !fixPairs) {
      firstPairRdd.map(r => AlignmentRecord.newBuilder(r)
        .setReadPaired(true)
        .setProperPair(true)
        .setFirstOfPair(true)
        .setSecondOfPair(false)
        .build()) ++ secondPairRdd.map(r => AlignmentRecord.newBuilder(r)
        .setReadPaired(true)
        .setProperPair(true)
        .setFirstOfPair(false)
        .setSecondOfPair(true)
        .build())
    } else {

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

      joinedRDD
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
    }

    // uncache temp rdds
    firstPairRdd.unpersist()
    secondPairRdd.unpersist()

    // return
    finalRdd
  }

  /**
   * Takes a sequence of Path objects (e.g. the return value of findFiles).  Treats each path as
   * corresponding to a Read set -- loads each Read set, converts each set to use the
   * same SequenceDictionary, and returns the union of the RDDs.
   *
   * (GenomeBridge is using this to load BAMs that have been split into multiple files per sample,
   * for example, one-BAM-per-chromosome.)
   *
   * @param paths The locations of the parquet files to load
   * @return a single RDD[Read] that contains the union of the AlignmentRecords in the argument paths.
   */
  def loadADAMFromPaths(paths: Seq[Path]): RDD[AlignmentRecord] = {
    def loadADAMs(path: Path): (SequenceDictionary, RDD[AlignmentRecord]) = {
      val dict = sc.adamDictionaryLoad[AlignmentRecord](path.toString)
      val rdd: RDD[AlignmentRecord] = sc.adamLoad(path.toString)
      (dict, rdd)
    }

    // Remapreferenceid code deleted since we don't remap sequence
    // dictionaries anymore.
    sc.union(paths.map(loadADAMs).map(v => v._2))
  }
}
