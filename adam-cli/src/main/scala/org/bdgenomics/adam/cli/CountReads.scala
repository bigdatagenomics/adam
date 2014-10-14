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
package org.bdgenomics.adam.cli

import java.io.IOException
import java.util.regex.Pattern

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceRecord
import org.bdgenomics.adam.projections.{ Projection, AlignmentRecordField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.features.FeaturesContext._
import org.bdgenomics.adam.rdd.features.ReadCounter
import org.bdgenomics.formats.avro.{ Feature, AlignmentRecord }
import org.kohsuke.args4j.Argument

object CountReads extends ADAMCommandCompanion {
  val commandName: String = "countreads"
  val commandDescription: String = "Reads a feature file and counts the number of overlapping reads-per-feature from an accompanying BAM/Parquet file"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new CountReads(Args4j[CountReadsArgs](cmdLine))
  }
}

class CountReadsArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "FEATURES", usage = "The Feature file to count by", index = 0)
  val featurePath: String = null

  @Argument(required = true, metaVar = "READS", usage = "The reads file to count", index = 1)
  val readPath: String = null
}

class CountReads(protected val args: CountReadsArgs) extends ADAMSparkCommand[CountReadsArgs] {
  val companion: ADAMCommandCompanion = CountReads

  def run(sc: SparkContext, job: Job): Unit = {

    val projection = Projection(
      AlignmentRecordField.readMapped,
      AlignmentRecordField.mateMapped,
      AlignmentRecordField.readPaired,
      AlignmentRecordField.contig,
      AlignmentRecordField.mateContig,
      AlignmentRecordField.primaryAlignment,
      AlignmentRecordField.duplicateRead,
      AlignmentRecordField.readMapped,
      AlignmentRecordField.mateMapped,
      AlignmentRecordField.firstOfPair,
      AlignmentRecordField.secondOfPair,
      AlignmentRecordField.properPair,
      AlignmentRecordField.mapq,
      AlignmentRecordField.start,
      AlignmentRecordField.end,
      AlignmentRecordField.cigar,
      AlignmentRecordField.failedVendorQualityChecks)

    try {
      // TODO(twd) should relax the assumption that these are in GTF format...
      val features: RDD[Feature] = sc.adamGTFFeatureLoad(args.featurePath)

      val reads: RDD[AlignmentRecord] = sc.adamLoad(args.readPath, projection = Some(projection))

      val dict = sc.adamDictionaryLoad[AlignmentRecord](args.readPath)

      val counted: RDD[(Feature, Int)] = ReadCounter.countReadsByFeature(sc, dict, features, reads)

      counted.map {
        case ((f: Feature, count: Int)) =>
          "% 10d\t%s:%d-%d\t%s".format(count,
            f.getContig.getContigName,
            f.getStart.toInt, f.getEnd.toInt,
            Option(f.getFeatureId).getOrElse(""))

      }.collect().foreach(println)

    } catch {

      case io: IOException =>

        val re = Pattern.compile("RuntimeException: (.*) is not a Parquet file")
        val matcher = re.matcher(io.getMessage)

        if (matcher.find()) {
          Console.err.println("ERROR: File \"%s\" doesn't appear to be a Parquet file.".format(matcher.group(1)))

        } else {
          throw io
        }

    }
  }
}
