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

import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }

/**
 * File that contains a reference assembly that can be broadcasted
 */
trait ReferenceFile extends Serializable {
  /**
   * Extract reference sequence from the file.
   *
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  def extract(region: ReferenceRegion): String

  /*
   * Stores SequenceDictionary for ReferenceFile
   */
  def references: SequenceDictionary
}
