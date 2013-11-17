/*
 * Copyright (c) 2013. The Broad Institute
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

import org.apache.spark.Logging
import org.apache.spark.broadcast.{Broadcast => SparkBroadcast}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.recalibration._
import org.apache.spark.rdd.RDD

private[rdd] object RecalibrateBaseQualities extends Serializable with Logging {

  def usableRead(read: ADAMRecord): Boolean = {
    // todo -- the mismatchingPositions should not merely be a filter, it should result in an exception. These are required for calculating mismatches.
    read.getReadMapped && read.getPrimaryAlignment && !read.getDuplicateRead && (read.getMismatchingPositions != null)
  }

  def apply(rdd: RDD[ADAMRecord], dbsnp: SparkBroadcast[Map[String, Set[Int]]]): RDD[ADAMRecord] = {
    // initialize the covariates
    println("Instantiating covariates...")
    val qualByRG = new QualByRG(rdd)
    val otherCovars = List(new DiscreteCycle(rdd), new BaseContext(rdd))
    println("Creating object...")
    val recalibrator = new RecalibrateBaseQualities(qualByRG, otherCovars)
    println("Computing table...")
    val table = recalibrator.computeTable(rdd.filter(usableRead), dbsnp)
    println("Applying table...")
    recalibrator.applyTable(table, rdd, qualByRG, otherCovars)
  }
}

private[rdd] class RecalibrateBaseQualities(val qualCovar: QualByRG, val covars: List[StandardCovariate]) extends Serializable with Logging {
  initLogging()

  def computeTable(rdd: RDD[ADAMRecord], dbsnp: SparkBroadcast[Map[String, Set[Int]]]): RecalTable = {

    def addCovariates(table: RecalTable, covar: ReadCovariates): RecalTable = {
      //log.info("Aggregating covarates for read "+covar.read.record.getReadName.toString)
      table + covar
    }

    def mergeTables(table1: RecalTable, table2: RecalTable): RecalTable = {
      log.info("Merging tables...")
      table1 ++ table2
    }

    rdd.map(r => ReadCovariates(r, qualCovar, covars, dbsnp)).aggregate(new RecalTable)(addCovariates, mergeTables)
  }

  def applyTable(table: RecalTable, rdd: RDD[ADAMRecord], qualCovar: QualByRG, covars: List[StandardCovariate]): RDD[ADAMRecord] = {
    table.finalizeTable()
    def recalibrate(record: ADAMRecord): ADAMRecord = {
      if (!record.getReadMapped || !record.getPrimaryAlignment || record.getDuplicateRead) {
        record // no need to recalibrate these records todo -- enable optional recalibration of all reads
      } else {
        RecalUtil.recalibrate(record, qualCovar, covars, table)
      }
    }
    rdd.map(recalibrate)
  }
}
