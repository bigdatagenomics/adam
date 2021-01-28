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
package org.bdgenomics.adam.ds.read.recalibration

import org.bdgenomics.adam.models.ReadGroupDictionary

/**
 * Table containing the empirical frequency of mismatches for each set of
 * covariate values.
 *
 * @param entries The error covariate &rarr; observed error frequency mapping.
 */
private[adam] class ObservationTable(
    val entries: scala.collection.Map[CovariateKey, Observation]) extends Serializable {

  override def toString = entries.map { case (k, v) => "%s\t%s".format(k, v) }.mkString("\n")

  /**
   * @param readGroups The read groups that generated the reads in this table.
   * @return Return this table as CSV.
   */
  def toCSV(readGroups: ReadGroupDictionary): String = {
    val rows = entries.map {
      case (key, obs) =>
        (CovariateSpace.toCSV(key, readGroups) ++
          obs.toCSV ++
          (if (key.containsNone) Seq("**") else Seq()))
    }
    (Seq(csvHeader) ++ rows).map(_.mkString(",")).mkString("\n")
  }

  private def csvHeader: Seq[String] = {
    (CovariateSpace.csvHeader ++
      Seq("TotalCount", "MismatchCount", "EmpiricalQ", "IsSkipped"))
  }
}
