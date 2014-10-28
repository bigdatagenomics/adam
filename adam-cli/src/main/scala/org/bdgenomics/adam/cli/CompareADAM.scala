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

import java.io.{ PrintWriter, OutputStreamWriter }
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.metrics.{ DefaultComparisons, CombinedComparisons, Collection, BucketComparisons }
import org.bdgenomics.adam.metrics.aggregators.{ HistogramAggregator, CombinedAggregator, AggregatedCollection, Writable }
import org.bdgenomics.adam.projections.AlignmentRecordField._
import org.bdgenomics.adam.projections.FieldValue
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.comparisons.ComparisonTraversalEngine
import org.bdgenomics.adam.util._
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import scala.collection.Seq

/**
 * CompareADAM is a tool for pairwise comparison of ADAM files (or merged sets of ADAM files, see the
 * note on the -recurse{1,2} optional parameters, below).
 *
 * The canonical use-case for CompareADAM involves a single input file run through (for example) two
 * different implementations of the same pipeline, producing two comparable ADAM files at the end.
 *
 * CompareADAM will load these ADAM files and perform a read-name-based equi-join.  It then computes
 * one or more metrics (embodied as BucketComparisons values) across the joined records, as specified
 * on the command-line, and aggregates each metric into a histogram (although, this can be modified if
 * other aggregations are required in the future) and outputs the resulting histograms to a specified
 * directory as text files.
 *
 * There is an R script in the adam-scripts module to process those outputs into a figure.
 *
 * The available metrics to be calculated are defined, by name, in the DefaultComparisons object.
 *
 * A subsequent tool like FindReads can be used to track down which reads give rise to particular aggregated
 * bins in the output histograms, if further diagnosis is needed.
 */
object CompareADAM extends ADAMCommandCompanion with Serializable {

  val commandName: String = "compare"
  val commandDescription: String = "Compare two ADAM files based on read name"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new CompareADAM(Args4j[CompareADAMArgs](cmdLine))
  }

  type GeneratedResults[A] = RDD[(String, Seq[A])]

  /**
   * @see CompareADAMArgs.recurse1, CompareADAMArgs.recurse2
   */
  def setupTraversalEngine(sc: SparkContext,
                           input1Path: String,
                           recurse1: String,
                           input2Path: String,
                           recurse2: String,
                           generator: BucketComparisons[Any]): ComparisonTraversalEngine = {

    val schemas = Seq[FieldValue](
      recordGroupId,
      readName,
      readMapped,
      primaryAlignment,
      readPaired,
      firstOfPair) ++ generator.schemas

    new ComparisonTraversalEngine(schemas, sc.findFiles(new Path(input1Path), recurse1), sc.findFiles(new Path(input2Path), recurse2))(sc)
  }

  def parseGenerators(nameList: String): Seq[BucketComparisons[Any]] = {
    nameList match {
      case null => DefaultComparisons.comparisons
      case s    => parseGenerators(s.split(","))
    }
  }

  def parseGenerators(names: Seq[String]): Seq[BucketComparisons[Any]] =
    names.map(DefaultComparisons.findComparison)
}

class CompareADAMArgs extends Args4jBase with ParquetArgs with Serializable {

  @Argument(required = true, metaVar = "INPUT1", usage = "The first ADAM file to compare", index = 0)
  val input1Path: String = null

  @Argument(required = true, metaVar = "INPUT2", usage = "The second ADAM file to compare", index = 1)
  val input2Path: String = null

  @Args4jOption(required = false, name = "-recurse1", metaVar = "REGEX",
    usage = "Optional regex; if specified, INPUT1 is recursively searched for directories matching this " +
      "pattern, whose contents are loaded and merged prior to running the comparison")
  val recurse1: String = null

  @Args4jOption(required = false, name = "-recurse2", metaVar = "REGEX",
    usage = "Optional regex; if specified, INPUT2 is recursively searched for directories matching this " +
      "pattern, whose contents are loaded and merged prior to running the comparison")
  val recurse2: String = null

  @Args4jOption(required = false, name = "-comparisons", usage = "Comma-separated list of comparisons to run")
  val comparisons: String = null

  @Args4jOption(required = false, name = "-list_comparisons",
    usage = "If specified, lists all the comparisons that are available")
  val listComparisons: Boolean = false

  @Args4jOption(required = false, name = "-output", metaVar = "DIRECTORY",
    usage = "Directory to generate the comparison output files (default: output to STDOUT)")
  val directory: String = null
}

class CompareADAM(protected val args: CompareADAMArgs) extends ADAMSparkCommand[CompareADAMArgs] with Serializable {

  val companion: ADAMCommandCompanion = CompareADAM

  /**
   * prints out a high-level summary of the compared files, including
   *
   * - the number of reads in each file
   *
   * - the number of reads which were unique to each file (i.e. not found in the other file, by name)
   *
   * - for each generator / aggregation created, prints out the total number of reads that went into the
   *   aggregation as well as the total which represent "identity" between the compared files (right now,
   *   this second number really only makes sense for the Histogram aggregated value, but that's the only
   *   aggregator we're using anyway.)
   *
   * @param engine The ComparisonTraversalEngine used for the aggregated values
   * @param writer The PrintWriter to print the summary with.
   */
  def printSummary(engine: ComparisonTraversalEngine,
                   generators: Seq[BucketComparisons[Any]],
                   aggregateds: Seq[Histogram[Any]],
                   writer: PrintWriter) {

    writer.println("%15s: %s".format("INPUT1", args.input1Path))
    writer.println("\t%15s: %d".format("total-reads", engine.named1.count()))
    writer.println("\t%15s: %d".format("unique-reads", engine.uniqueToNamed1()))

    writer.println("%15s: %s".format("INPUT2", args.input2Path))
    writer.println("\t%15s: %d".format("total-reads", engine.named2.count()))
    writer.println("\t%15s: %d".format("unique-reads", engine.uniqueToNamed2()))

    for ((generator, aggregated) <- generators.zip(aggregateds)) {
      writer.println()
      writer.println(generator.name)

      val count = aggregated.count()
      val countIdentity = aggregated.countIdentical()
      val diffFrac = (count - countIdentity).toDouble / count.toDouble

      writer.println("\t%15s: %d".format("count", count))
      writer.println("\t%15s: %d".format("identity", countIdentity))
      writer.println("\t%15s: %.5f".format("diff%", 100.0 * diffFrac))
    }
  }

  def run(sc: SparkContext, job: Job): Unit = {

    if (args.listComparisons) {
      println("\nAvailable comparisons:")
      DefaultComparisons.comparisons.foreach {
        generator =>
          println("\t%10s : %s".format(generator.name, generator.description))
      }

      return
    }

    val generators: Seq[BucketComparisons[Any]] = CompareADAM.parseGenerators(args.comparisons)
    val aggregators = (0 until generators.size).map(i => new HistogramAggregator[Any]())

    val generator = new CombinedComparisons(generators)
    val aggregator = new CombinedAggregator(aggregators)

    // Separately constructing the traversal engine, since we'll need it to generate the aggregated values
    // and the summary statistics separately.
    val engine = CompareADAM.setupTraversalEngine(sc,
      args.input1Path,
      args.recurse1,
      args.input2Path,
      args.recurse2,
      generator)

    // generate the raw values...
    val generated: CompareADAM.GeneratedResults[Collection[Seq[Any]]] = engine.generate(generator)

    // ... and aggregate them.
    val values: AggregatedCollection[Any, Histogram[Any]] = ComparisonTraversalEngine.combine(generated, aggregator)

    val aggValues = values.asInstanceOf[AggregatedCollection[Any, Histogram[Any]]].values

    if (args.directory != null) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val nameOutput = new OutputStreamWriter(fs.create(new Path(args.directory, "files")))
      nameOutput.write(args.input1Path)
      nameOutput.write("\n")
      nameOutput.write(args.input2Path)
      nameOutput.write("\n")
      nameOutput.close()

      val summaryOutput = new PrintWriter(new OutputStreamWriter(fs.create(new Path(args.directory, "summary.txt"))))
      printSummary(engine, generators, aggValues, summaryOutput)
      summaryOutput.close()

      generators.zip(aggValues).foreach {
        case (gen, value) => {
          val output = new OutputStreamWriter(fs.create(new Path(args.directory, gen.name)))
          value.asInstanceOf[Writable].write(output)

          output.close()
        }
      }
    } else {

      // Just send everything to STDOUT if the directory isn't specified.
      val writer = new PrintWriter(System.out)

      printSummary(engine, generators, aggValues, writer)

      generators.zip(aggValues).foreach {
        case (gen, value) =>
          writer.println(gen.name)
          writer.println(value)
      }
    }
  }

}

