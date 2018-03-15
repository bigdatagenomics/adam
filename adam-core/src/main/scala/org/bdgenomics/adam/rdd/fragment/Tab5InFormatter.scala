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
import org.bdgenomics.adam.sql.{ Fragment => FragmentProduct }
import org.bdgenomics.formats.avro.Fragment
import org.bdgenomics.utils.misc.Logging

/**
 * InFormatter companion that creates an InFormatter that writes Bowtie tab5 format.
 */
object Tab5InFormatter extends InFormatterCompanion[Fragment, FragmentProduct, FragmentRDD, Tab5InFormatter] {

  /**
   * Builds an Tab5InFormatter to write Bowtie tab5 format.
   *
   * @param gRdd GenomicRDD of Fragments. Used to get HadoopConfiguration.
   * @return Returns a new Tab6InFormatter.
   */
  def apply(gRdd: FragmentRDD): Tab5InFormatter = {
    new Tab5InFormatter(gRdd.rdd.context.hadoopConfiguration)
  }
}

class Tab5InFormatter private (
    conf: Configuration) extends InFormatter[Fragment, FragmentProduct, FragmentRDD, Tab5InFormatter] with Logging {

  protected val companion = Tab5InFormatter
  private val newLine = "\n".getBytes
  private val converter = new AlignmentRecordConverter
  private val writeOriginalQualities = conf.getBoolean(FragmentRDD.WRITE_ORIGINAL_QUALITIES, false)

  /**
   * Writes alignment records to an output stream in Bowtie tab5 format.
   *
   * In Bowtie tab5 format, each alignment record or pair is on a single line.
   * An unpaired alignment record line is [name]\t[seq]\t[qual]\n.
   * A paired-end read line is [name]\t[seq1]\t[qual1]\t[seq2]\t[qual2]\n.
   *
   * The read name for a paired-end read line is the name of the first
   * read with the suffix trimmed.
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
          log.warn("More than two reads for %s. Taking first 2.".format(frag))
        }
        reads.take(2)
      }
    }).foreach(reads => {

      // write unpaired read or first of paired-end reads
      val first = converter.convertToTab5(reads(0),
        outputOriginalBaseQualities = writeOriginalQualities)

      os.write(first.getBytes)

      // write second of paired-end reads, if present
      if (reads.size > 1) {
        val second = "\t" + converter.convertSecondReadToTab5(reads(1),
          outputOriginalBaseQualities = writeOriginalQualities)

        os.write(second.getBytes)
      }

      // end line
      os.write(newLine)
    })
  }
}
