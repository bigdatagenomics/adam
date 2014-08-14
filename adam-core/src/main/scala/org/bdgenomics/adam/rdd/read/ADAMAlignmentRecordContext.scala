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

import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import fi.tkk.ics.hadoop.bam.{ AnySAMInputFormat, SAMRecordWritable }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.formats.avro.AlignmentRecord
import parquet.hadoop.util.ContextUtil

object ADAMAlignmentRecordContext {
  // Add ADAM Spark context methods
  implicit def adamContextToADAMContext(ac: ADAMContext): ADAMAlignmentRecordContext = new ADAMAlignmentRecordContext(ac.sc)

  // Add methods specific to Read RDDs
  implicit def rddToADAMRecordRDD(rdd: RDD[AlignmentRecord]) = new ADAMAlignmentRecordRDDFunctions(rdd)
}

class ADAMAlignmentRecordContext(val sc: SparkContext) extends Serializable with Logging {

  protected[rdd] def adamBamLoad(filePath: String): RDD[AlignmentRecord] = {
    log.info("Reading legacy BAM file format %s to create RDD".format(filePath))

    // We need to separately read the header, so that we can inject the sequence dictionary
    // data into each individual Read (see the argument to samRecordConverter.convert,
    // below).
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    val seqDict = sc.adamBamDictionaryLoad(samHeader)
    val readGroups = sc.adamBamLoadReadGroups(samHeader)

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    val samRecordConverter = new SAMRecordConverter
    records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups))
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
   * @return a single RDD[Read] that contains the union of the ADAMRecords in the argument paths.
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
