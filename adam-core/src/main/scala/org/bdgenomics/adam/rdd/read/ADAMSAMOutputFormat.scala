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
package org.bdgenomics.adam.rdd.read

import org.seqdoop.hadoop_bam.{ SAMRecordWritable, KeyIgnoringAnySAMOutputFormat, SAMFormat }
import htsjdk.samtools.SAMFileHeader
import org.apache.spark.rdd.InstrumentedOutputFormat
import org.bdgenomics.adam.instrumentation.Timers
import org.apache.hadoop.mapreduce.OutputFormat

object ADAMSAMOutputFormat extends Serializable {

  private[read] var header: Option[SAMFileHeader] = None

  /**
   * Attaches a header to the ADAMSAMOutputFormat Hadoop writer. If a header has previously
   * been attached, the header must be cleared first.
   *
   * @throws Exception Exception thrown if a SAM header has previously been attached, and not cleared.
   *
   * @param samHeader Header to attach.
   *
   * @see clearHeader
   */
  def addHeader(samHeader: SAMFileHeader) {
    assert(header.isEmpty, "Cannot attach a new SAM header without first clearing the header.")
    header = Some(samHeader)
  }

  /**
   * Clears the attached header.
   *
   * @see addHeader
   */
  def clearHeader() {
    header = None
  }

  /**
   * Returns the current header.
   *
   * @return Current SAM header.
   */
  private[read] def getHeader: SAMFileHeader = {
    assert(header.isDefined, "Cannot return header if not attached.")
    header.get
  }
}

class ADAMSAMOutputFormat[K]
    extends KeyIgnoringAnySAMOutputFormat[K](SAMFormat.valueOf("SAM")) with Serializable {

  setSAMHeader(ADAMSAMOutputFormat.getHeader)
}

class InstrumentedADAMSAMOutputFormat[K] extends InstrumentedOutputFormat[K, org.seqdoop.hadoop_bam.SAMRecordWritable] {
  override def timerName(): String = Timers.WriteSAMRecord.timerName
  override def outputFormatClass(): Class[_ <: OutputFormat[K, SAMRecordWritable]] = classOf[ADAMSAMOutputFormat[K]]
}
