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

import org.bdgenomics.adam.rdd.ADAMContext._
import org.kohsuke.args4j.{ Option, Argument }
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.projections.{ Projection, ADAMRecordField }
import org.apache.spark.rdd.RDD
import ADAMRecordField._

/**
 * Reads in the tagStrings field of every record, and prints out the set of unique
 * tags found in those fields along with the number of records that have each particular
 * tag.
 */
object PrintTags extends ADAMCommandCompanion {
  val commandName: String = "print_tags"
  val commandDescription: String = "Prints the values and counts of all tags in a set of records"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new PrintTags(Args4j[PrintTagsArgs](cmdLine))
  }
}

class PrintTagsArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to scan for tags", index = 0)
  val inputPath: String = null

  @Option(required = false, name = "-list",
    usage = "When value is set to <N>, also lists the first N attribute fields for ADAMRecords in the input")
  var list: String = null

  @Option(required = false, name = "-count",
    usage = "comma-separated list of tag names; for each tag listed, we print the distinct values and their counts")
  var count: String = null

}

class PrintTags(protected val args: PrintTagsArgs) extends ADAMSparkCommand[PrintTagsArgs] {
  val companion: ADAMCommandCompanion = PrintTags

  def run(sc: SparkContext, job: Job): Unit = {
    val toCount = if (args.count != null) args.count.split(",").toSet else Set()

    val proj = Projection(attributes, primaryAlignment, readMapped, readPaired, failedVendorQualityChecks)
    val rdd: RDD[ADAMRecord] = sc.adamLoad(args.inputPath, projection = Some(proj))
    val filtered = rdd.filter(rec => !rec.getFailedVendorQualityChecks)

    if (args.list != null) {
      val count = args.list.toInt
      filtered.take(count).map(_.getAttributes).foreach(println)
    }

    val tagCounts = filtered.adamCharacterizeTags().collect()
    for ((tag, count) <- tagCounts) {
      println("%3s\t%d".format(tag, count))
      if (toCount.contains(tag)) {
        val countMap = filtered.adamCharacterizeTagValues(tag)
        for ((value, valueCount) <- countMap) {
          println("\t%10d\t%s".format(valueCount, value.toString))
        }
      }
    }

    println("Total: %d".format(filtered.count()))
  }

}
