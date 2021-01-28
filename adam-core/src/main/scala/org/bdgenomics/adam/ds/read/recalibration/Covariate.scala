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

import org.bdgenomics.formats.avro.Alignment

/**
 * A Covariate represents a predictor, also known as a "feature" or
 * "independent variable".
 *
 * @tparam T The type of this feature.
 */
private[recalibration] abstract class Covariate[T] {

  /**
   * Given a read, computes the value of this covariate for each residue in the
   * read.
   *
   * @param read The read to observe.
   * @return The covariates corresponding to each base in this read.
   */
  def compute(read: Alignment): Array[T]

  /**
   * Format the provided covariate value to be compatible with GATK's CSV output.
   *
   * @param cov A covariate value to render.
   * @return Returns the covariate value rendered as a single CSV cell.
   */
  def toCSV(cov: T): String = {
    cov.toString
  }

  /**
   * A short name for this covariate, used in CSV output header.
   */
  val csvFieldName: String
}
