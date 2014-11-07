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
package org.bdgenomics.adam.util

import htsjdk.samtools.{ SAMFormatException, ValidationStringency }
import org.apache.spark.Logging

trait ValidationLogging extends Logging {
  def validationStringency: ValidationStringency

  // Adapted from HTSJDK's SAMLineConverter
  def error(reason: String) {
    if (validationStringency == ValidationStringency.STRICT) {
      throw new SAMFormatException(reason)
    } else if (validationStringency == ValidationStringency.LENIENT) {
      log.warn("Ignoring SAM validation error due to lenient parsing:")
      log.warn(reason)
    }
  }

  def errorIf(condition: Boolean, reason: String) =
    if (condition)
      error(reason)
}
