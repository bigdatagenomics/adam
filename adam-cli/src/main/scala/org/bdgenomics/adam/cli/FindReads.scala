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

import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import scala.collection.Seq
import java.util.regex.Pattern
import org.apache.hadoop.fs.{ Path, FileSystem }
import java.io.OutputStreamWriter
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReadBucket
import org.bdgenomics.adam.metrics.{ DefaultComparisons, BucketComparisons }
import org.bdgenomics.adam.metrics.filters.{ CombinedFilter, GeneratorFilter }

/**
 * FindReads is an auxiliary command to CompareADAM -- whereas CompareADAM takes two ADAM files (which
 * presumably contain the same reads, processed differently), joins them based on read-name, and computes
 * aggregates of one or more metrics (BucketComparisons) across those joined reads -- FindReads performs
 * the same join-and-metric-computation, but takes a second argument as well: a boolean condition on the
 * value(s) of the metric(s) computed.  FindReads then outputs just the names of the reads whose metric
 * values(s) meet the given condition.
 *
 * So, for example, CompareADAM might be used to find out that the same reads processed through two different
 * pipelines are aligned to different locations 3% of the time.
 *
 * FindReads would then allow you to output the names of those 3% of the reads which are aligned differently,
 * using a filter expression like "positions!=0".
 */
object FindReads extends ADAMCommandCompanion {
  val commandName: String = "findreads"
  val commandDescription: String = "Find reads that match particular individual or comparative criteria"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new FindReads(Args4j[FindReadsArgs](cmdLine))
  }

  private val filterRegex = Pattern.compile("([^!=<>]+)((!=|=|<|>).*)")

  /**
   * A 'filter' is a pair of a BucketComparisons value and a boolean condition on the values output
   * by the comparison.  The BucketComparisons object is specified by name, and parsed by the
   * findGenerator method in DefaultComparisons that maps strings to BucketComparisons objects.
   *
   * The boolean condition is specified by an operator { !=, =, <, > } and a value (which is
   * an integer, floating point number, pair specified by the '(value,value)' notation, or boolean).
   *
   * For example, "positions!=0" would specify those reads whose corresponding distance between files
   * was non-zero.
   *
   * Again, "dupemismatch=(1,0)" would specify those reads which are marked as a duplicate in the first
   * file and _not_ in the second file.
   *
   * @param filterString A String conforming to the regex "[^^!=<>]+(!=|=|<|>).*"
   * @return A GeneratorFilter object which contains both the BucketComparisons value and the filter condition.
   */
  def parseFilter(filterString: String): GeneratorFilter[Any] = {

    val matcher = filterRegex.matcher(filterString)
    if (!matcher.matches()) { throw new IllegalArgumentException(filterString) }

    val generator: BucketComparisons[Any] = DefaultComparisons.findComparison(matcher.group(1))
    val filterDef = matcher.group(2)

    generator.createFilter(filterDef)
  }

  def parseFilters(filters: String): GeneratorFilter[Any] =
    new CombinedFilter[Any](filters.split(";").map(parseFilter))
}

class FindReadsArgs extends Args4jBase with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "INPUT1", usage = "The first ADAM file to compare", index = 0)
  val input1Path: String = null

  @Argument(required = true, metaVar = "INPUT2", usage = "The second ADAM file to compare", index = 1)
  val input2Path: String = null

  @Argument(required = true, metaVar = "FILTER", usage = "Filter to run", index = 2)
  val filter: String = null

  @Args4jOption(required = false, name = "-recurse1",
    usage = "Optional regex; if specified, INPUT1 is recursively searched for directories matching this " +
      "pattern, whose contents are loaded and merged prior to running the comparison")
  val recurse1: String = null

  @Args4jOption(required = false, name = "-recurse2",
    usage = "Optional regex; if specified, INPUT2 is recursively searched for directories matching this " +
      "pattern, whose contents are loaded and merged prior to running the comparison")
  val recurse2: String = null

  @Args4jOption(required = false, name = "-file", usage = "File name to write the matching read names to")
  val file: String = null
}

class FindReads(protected val args: FindReadsArgs) extends ADAMSparkCommand[FindReadsArgs] with Serializable {
  val companion: ADAMCommandCompanion = FindReads

  def run(sc: SparkContext, job: Job): Unit = {

    val filter = FindReads.parseFilters(args.filter)
    val generator = filter.comparison

    val engine = CompareADAM.setupTraversalEngine(sc, args.input1Path, args.recurse1, args.input2Path, args.recurse2, generator)

    val generated: CompareADAM.GeneratedResults[Any] = engine.generate(generator)

    val filtered: RDD[(String, Seq[Any])] = generated.filter {
      case (name: String, values: Seq[Any]) =>
        values.exists(filter.passesFilter)
    }

    val filteredJoined = engine.joined.join(filtered).map {
      case (name: String, ((bucket1: ReadBucket, bucket2: ReadBucket), generated: Seq[Any])) => {
        val rec1 = bucket1.allReads().head
        val rec2 = bucket2.allReads().head
        (name, "%s:%d".format(rec1.getContig.getContigName, rec1.getStart), "%s:%d".format(rec2.getContig.getContigName, rec2.getStart))
      }
    }

    if (args.file != null) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val file = new OutputStreamWriter(fs.create(new Path(args.file)))
      file.write(generator.name)
      file.write("\n")
      for (value <- filteredJoined.collect()) {
        file.write(Seq(value._1, value._2, value._3).mkString("\t"))
        file.write("\n")
      }
    } else {
      // TODO generalize for files.
      println(generator.name)
      for (value <- filteredJoined.collect()) {
        println(Seq(value._1, value._2, value._3).mkString("\t"))
      }
    }

  }

}
