/*
 * Copyright (c) 2014 The Regents of the University of California
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

package org.bdgenomics.adam.rdd.recalibration

import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.recalibration._
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.rich.DecadentRead._
import org.bdgenomics.adam.util.QualityScore
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Base Quality Score Recalibration
 * ================================
 *
 * The algorithm proceeds in two phases:
 *
 *  1. Make a pass over the reads to collect statistics and build the
 *     recalibration tables.
 *
 *  2. Perform a second pass over the reads to apply the recalibration and
 *     assign adjusted quality scores.
 */
class BaseQualityRecalibration(
  val input: RDD[DecadentRead],
  val knownSnps: Broadcast[SnpTable])
    extends Serializable with Logging {

  // Additional covariates to use when computing the correction
  // TODO: parameterize
  val covariates = CovariateSpace(
    new DinucCovariate)

  // Bases with quality less than this will be skipped and left alone
  // TODO: parameterize
  val minAcceptableQuality = QualityScore(5)

  // debug flags
  val dumpObservationTable = false
  val enableVisitLogging = false

  val dataset: RDD[(CovariateKey, Residue)] = {
    def shouldIncludeRead(read: DecadentRead) =
      read.isCanonicalRecord &&
        read.alignmentQuality.exists(_ > QualityScore.zero) &&
        read.passedQualityChecks

    def shouldIncludeResidue(residue: Residue) =
      residue.quality > QualityScore.zero &&
        residue.isRegularBase &&
        !residue.isInsertion &&
        !knownSnps.value.isMasked(residue)

    def observe(read: DecadentRead): Seq[(CovariateKey, Residue)] =
      covariates(read).zip(read.sequence).
        filter { case (key, residue) => shouldIncludeResidue(residue) }

    input.filter(shouldIncludeRead).flatMap(observe)
  }

  if (enableVisitLogging) {
    input.cache
    dataset.cache
    dumpVisits("bqsr-visits.dump")
  }

  val observed: ObservationTable = {
    dataset.
      map { case (key, residue) => (key, Observation(residue.isSNP)) }.
      aggregate(ObservationAccumulator(covariates))(_ += _, _ ++= _).result
  }

  if (dumpObservationTable) {
    println(observed.toCSV)
  }

  val result: RDD[ADAMRecord] = {
    val recalibrator = Recalibrator(observed, minAcceptableQuality)
    input.map(recalibrator)
  }

  private def dumpVisits(filename: String) = {
    def readId(read: DecadentRead): String =
      read.name +
        (if (read.isNegativeRead) "-" else "+") +
        (if (read.record.getFirstOfPair) "1" else "") +
        (if (read.record.getSecondOfPair) "2" else "")

    val readLengths =
      input.map(read => (readId(read), read.sequence.length)).collectAsMap

    val visited = dataset.
      map { case (key, residue) => (readId(residue.read), Seq(residue.position)) }.
      reduceByKeyLocally((left, right) => left ++ right)

    val outf = new java.io.File(filename)
    val writer = new java.io.PrintWriter(outf)
    visited.foreach {
      case (readName, visited) =>
        val length = readLengths(readName)
        val buf = Array.fill[Char](length)('O')
        visited.foreach { idx => buf(idx) = 'X' }
        writer.println(readName + "\t" + String.valueOf(buf))
    }
    writer.close
  }
}

object BaseQualityRecalibration {
  def apply(rdd: RDD[ADAMRecord], knownSnps: Broadcast[SnpTable]): RDD[ADAMRecord] =
    new BaseQualityRecalibration(cloy(rdd), knownSnps).result
}
