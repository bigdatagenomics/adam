/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.util

import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMOutputFormat
import net.sf.samtools.SAMFileHeader

object SparkBAMOutputFormat {
  var header = new SAMFileHeader

  def setHeader (head: SAMFileHeader) {
    header = head
  }

  def getHeader () = header
}

class SparkBAMOutputFormat[K] extends KeyIgnoringBAMOutputFormat[K] {
  setSAMHeader(SparkBAMOutputFormat.getHeader)
}
