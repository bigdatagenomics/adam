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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.adam.predicates.GenotypeRecordPASSPredicate
import org.bdgenomics.adam.projections.GenotypeField
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ConcordanceTable
import org.bdgenomics.formats.avro.Genotype

object GenotypeConcordance extends ADAMCommandCompanion {
  val commandName = "genotype_concordance"
  val commandDescription = "Pairwise comparison of sets of ADAM genotypes"

  def apply(cmdLine: Array[String]) = {
    new GenotypeConcordance(Args4j[GenotypeConcordanceArgs](cmdLine))
  }
}

class GenotypeConcordanceArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "test", usage = "The test ADAM genotypes file", index = 0)
  var testGenotypesFile: String = _
  @Argument(required = true, metaVar = "truth", usage = "The truth ADAM genotypes file", index = 1)
  var truthGenotypesFile: String = _
  @Args4jOption(required = false, name = "-include_non_pass", usage = "Include non-PASSing sites in concordance evaluation")
  var includeNonPass: Boolean = false
}

class GenotypeConcordance(protected val args: GenotypeConcordanceArgs) extends ADAMSparkCommand[GenotypeConcordanceArgs] with Logging {
  val companion: ADAMCommandCompanion = GenotypeConcordance

  def run(sc: SparkContext, job: Job): Unit = {
    // TODO: Figure out projections of nested fields
    var project = List(
      GenotypeField.variant, GenotypeField.sampleId, GenotypeField.alleles)

    val predicate = if (!args.includeNonPass) {
      // We also need to project the filter field to use this predicate
      // project :+= varIsFiltered
      Some(classOf[GenotypeRecordPASSPredicate])
    } else
      None
    val projection = None //Some(Projection(project))

    val testGTs: RDD[Genotype] = sc.adamLoad(args.testGenotypesFile, predicate, projection)
    val truthGTs: RDD[Genotype] = sc.adamLoad(args.truthGenotypesFile, predicate, projection)

    val tables = testGTs.concordanceWith(truthGTs)

    // Write out results as a table
    System.out.println("Sample\tConcordance\tNonReferenceSensitivity")
    for ((sample, table) <- tables.collectAsMap()) {
      System.out.println("%s\t%f\t%f".format(sample, table.concordance, table.nonReferenceSensitivity))
    }
    {
      val total = tables.values.fold(ConcordanceTable())((c1, c2) => c1.add(c2))
      System.out.println("ALL\t%f\t%f".format(total.concordance, total.nonReferenceSensitivity))
    }

  }
}
