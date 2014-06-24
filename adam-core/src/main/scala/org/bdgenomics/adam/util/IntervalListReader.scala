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

import java.io.{ FileInputStream, File }
import scala.collection._
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import net.sf.picard.util.IntervalList
import scala.collection.JavaConverters._
import net.sf.samtools.SAMTextHeaderCodec
import net.sf.samtools.util.BufferedLineReader

/**
 * Reads GATK-style interval list files
 * e.g. example file taken from this page:
 * http://www.broadinstitute.org/gatk/guide/article?id=1204
 *
 * @param file a File whose contents are a (line-based tab-separated) interval file in UTF-8 encoding
 */
class IntervalListReader(file: File) extends Traversable[(ReferenceRegion, String)] {
  val encoding = "UTF-8"

  /**
   * The interval list file contains a SAM-style sequence dictionary as a header --
   * this value holds that dictionary, reading it if necessary (if the file hasn't been
   * opened before).
   */
  lazy val sequenceDictionary: SequenceDictionary = {
    // Do we need to manually close the file stream?
    val codec = new SAMTextHeaderCodec()
    SequenceDictionary(codec.decode(new BufferedLineReader(new FileInputStream(file)), file.toString))
  }

  def foreach[U](f: ((ReferenceRegion, String)) => U) {
    IntervalList.fromFile(file).asScala.foreach(
      i => f((ReferenceRegion(i.getSequence, i.getStart, i.getEnd), i.getName)))
  }
}
