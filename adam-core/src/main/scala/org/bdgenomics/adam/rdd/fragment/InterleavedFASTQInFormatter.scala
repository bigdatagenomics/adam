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
package org.bdgenomics.adam.rdd.fragment

import java.io.OutputStream
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.rdd.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.formats.avro.Fragment
import org.bdgenomics.utils.misc.Logging

object InterleavedFASTQInFormatter extends InFormatterCompanion[Fragment, FragmentRDD, InterleavedFASTQInFormatter] {

  val WRITE_ORIGINAL_QUAL = "org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter.writeOriginalQual"
  val WRITE_SUFFIXES = "org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter.writeSuffixes"

  def apply(gRdd: FragmentRDD): InterleavedFASTQInFormatter = {
    new InterleavedFASTQInFormatter(gRdd.rdd.context.hadoopConfiguration)
  }
}

class InterleavedFASTQInFormatter(conf: Configuration) extends InFormatter[Fragment, FragmentRDD, InterleavedFASTQInFormatter] with Logging {

  protected val companion = InterleavedFASTQInFormatter
  private val converter = new AlignmentRecordConverter
  private val writeSuffixes = conf.getBoolean(InterleavedFASTQInFormatter.WRITE_SUFFIXES, false)
  private val writeOQ = conf.getBoolean(InterleavedFASTQInFormatter.WRITE_ORIGINAL_QUAL, false)

  /**
   * Writes alignment records to an output stream in interleaved FASTQ format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[Fragment]) {
    iter.flatMap(frag => {
      val reads = converter.convertFragment(frag).toSeq

      if (reads.size < 2) {
        log.warn("Fewer than two reads for %s. Dropping...".format(frag))
        None
      } else {
        if (reads.size > 2) {
          log.warn("More than two reads for %s. Taking first 2.".format(frag))
        }
        Some((reads(0), reads(1)))
      }
    }).foreach(p => {
      val (read1, read2) = p

      // convert both reads to fastq
      val fastq1 = converter.convertToFastq(read1,
        maybeAddSuffix = writeSuffixes,
        outputOriginalBaseQualities = writeOQ) + "\n"
      val fastq2 = converter.convertToFastq(read2,
        maybeAddSuffix = writeSuffixes,
        outputOriginalBaseQualities = writeOQ) + "\n"

      // write both to the output stream
      os.write(fastq1.getBytes)
      os.write(fastq2.getBytes)
    })
  }
}
