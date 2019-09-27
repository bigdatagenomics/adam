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
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.rdd.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.adam.sql.{ Fragment => FragmentProduct }
import org.bdgenomics.formats.avro.Fragment

/**
 * InFormatter companion that creates an InFormatter that writes Bowtie tab6 format.
 */
object Tab6InFormatter extends InFormatterCompanion[Fragment, FragmentProduct, FragmentDataset, Tab6InFormatter] {

  /**
   * Builds an Tab6InFormatter to write Bowtie tab6 format.
   *
   * @param gDataset GenomicDataset of Fragments. Used to get HadoopConfiguration.
   * @return Returns a new Tab6InFormatter.
   */
  def apply(gDataset: FragmentDataset): Tab6InFormatter = {
    new Tab6InFormatter(gDataset.rdd.context.hadoopConfiguration)
  }
}

class Tab6InFormatter private (
    conf: Configuration) extends InFormatter[Fragment, FragmentProduct, FragmentDataset, Tab6InFormatter] with Logging {

  protected val companion = Tab6InFormatter
  private val newLine = "\n".getBytes
  private val converter = new AlignmentConverter
  private val writeSuffixes = conf.getBoolean(FragmentDataset.WRITE_SUFFIXES, false)
  private val writeOriginalQualityScores = conf.getBoolean(FragmentDataset.WRITE_ORIGINAL_QUALITY_SCORES, false)

  /**
   * Writes alignments to an output stream in Bowtie tab6 format.
   *
   * In Bowtie tab6 format, each alignment or pair is on a single line.
   * An unpaired alignment line is [name]\t[seq]\t[qual]\n.
   * For paired-end alignments, the second end can have a different name
   * from the first: [name1]\t[seq1]\t[qual1]\t[name2]\t[seq2]\t[qual2]\n.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[Fragment]) {
    iter.map(frag => {
      val reads = converter.convertFragment(frag).toSeq

      if (reads.size < 2) {
        reads
      } else {
        if (reads.size > 2) {
          warn("More than two reads for %s. Taking first 2.".format(frag))
        }
        reads.take(2)
      }
    }).foreach(reads => {

      // write unpaired read or first of paired-end reads
      val first = converter.convertToTab6(reads(0),
        maybeAddSuffix = writeSuffixes,
        writeOriginalQualityScores)

      os.write(first.getBytes)

      // write second of paired-end reads, if present
      if (reads.size > 1) {
        val second = "\t" + converter.convertToTab6(reads(1),
          maybeAddSuffix = writeSuffixes,
          writeOriginalQualityScores)

        os.write(second.getBytes)
      }

      // end line
      os.write(newLine)
    })
  }
}
