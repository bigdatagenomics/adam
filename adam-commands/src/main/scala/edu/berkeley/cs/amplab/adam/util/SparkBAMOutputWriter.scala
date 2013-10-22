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

  /**
   * SMELL justification:
   * This class is passed to the Spark RDD-->Hadoop file writer, which creates a class instance by calling the
   * default constructor from inside of the writer. As a result, we cannot pass any parameters to the constructor,
   * or call any setters after the constructor. However, Hadoop-BAM wraps Picard which requires a header to be
   * associated with the SAM file before the file can be written; if a header is not attached, the write will
   * throw an exception. To work around this, we use a static method to initialize the header.
   */
  def setHeader(head: SAMFileHeader) {
    header = head
  }

  def getHeader = header
}

class SparkBAMOutputFormat[K] extends KeyIgnoringBAMOutputFormat[K] {
  /* SMELL: see accompanying note in companion object */
  setSAMHeader(SparkBAMOutputFormat.getHeader)
}
