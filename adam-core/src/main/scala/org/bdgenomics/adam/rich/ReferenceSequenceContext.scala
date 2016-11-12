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
package org.bdgenomics.adam.rich

import htsjdk.samtools.CigarElement
import org.bdgenomics.adam.models.ReferencePosition

/**
 * Represents information on the reference relative to a particular residue
 *
 * @param pos The position this residue is aligned at.
 * @param referenceBase The base that is in the reference at this position
 * @param cigarElement The CIGAR element covering this residue in the
 *   read-to-reference alignment.
 * @param cigarElementOffset The position of this base within the CIGAR
 *   element.
 */
@deprecated("don't use ReferenceSequenceContext in new development",
  since = "0.21.0")
private[adam] case class ReferenceSequenceContext(
    pos: Option[ReferencePosition],
    referenceBase: Option[Char],
    cigarElement: CigarElement,
    cigarElementOffset: Int) {
}
