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
package org.bdgenomics.adam.converters

import org.bdgenomics.adam.util._
import scala.math.{ pow, sqrt }

private[adam] class GenotypesToVariantsConverter(
  validateSamples: Boolean = false,
    failOnValidationError: Boolean = false
) extends Serializable {

  /**
   * Computes root mean squared (RMS) values for a series of doubles.
   *
   * @param values A series of doubles.
   * @return The RMS of this series.
   */
  def rms(values: Seq[Double]): Double = {
    if (values.length > 0) {
      sqrt(values.map(pow(_, 2.0)).sum / values.length.toDouble)
    } else {
      0.0
    }
  }

  /**
   * Computes root mean squared (RMS) values for a series of phred scaled quality scores.
   *
   * @param values A series of phred scores.
   * @return The RMS of this series.
   */
  def rms(values: Seq[Int]): Int = {
    if (values.length > 0) {
      PhredUtils.successProbabilityToPhred(rms(values.map(PhredUtils.phredToSuccessProbability)))
    } else {
      0
    }
  }

  /**
   * Finds variant quality from genotype qualities. Variant quality is defined as the likelihood
   * that at least 1 variant exists in the set of samples we have seen. This can be rephrased as
   * the likelihood that there are not 0 variants in the set of samples we have seen. We can
   * assume that all of our genotypes are Bernouli with p=genotype quality. Then, this calculation
   * becomes:
   *
   * P(X = 0) = product, g in genotypes -> (1 - Pg)
   * P(X > 0) = 1 - P(X = 0)
   *
   * Where Pg is the per genotype likelihood that the genotype is correct, and X is the number
   * of times we see a variant.
   *
   * @param values An array of non-phred scaled genotype quality scores.
   * @return A non-phred scaled variant likelihood.
   */
  def variantQualityFromGenotypes(values: Seq[Double]): Double = 1.0 - values.product
}
