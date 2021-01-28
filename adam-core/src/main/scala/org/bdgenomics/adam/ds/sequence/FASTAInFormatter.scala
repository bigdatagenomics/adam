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
package org.bdgenomics.adam.ds.sequence

import java.io.OutputStream
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.adam.ds.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.adam.sql.{ Sequence => SequenceProduct }
import org.bdgenomics.formats.avro.Sequence

/**
 * InFormatter companion that creates an InFormatter that writes FASTA.
 */
object FASTAInFormatter extends InFormatterCompanion[Sequence, SequenceProduct, SequenceDataset, FASTAInFormatter] {

  /**
   * Builds a FASTAInFormatter to write FASTA.
   *
   * @param gDataset GenomicDataset of Sequences. Used to get HadoopConfiguration.
   * @return Returns a new FASTA InFormatter.
   */
  def apply(gDataset: SequenceDataset): FASTAInFormatter = {
    new FASTAInFormatter(gDataset.rdd.context.hadoopConfiguration)
  }
}

class FASTAInFormatter private (
    conf: Configuration) extends InFormatter[Sequence, SequenceProduct, SequenceDataset, FASTAInFormatter] {

  protected val companion = FASTAInFormatter
  private val lineWidth = conf.getInt(SequenceDataset.FASTA_LINE_WIDTH, 60)

  /**
   * Writes sequences to an output stream in FASTA format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[Sequence]) {
    def toFasta(sequence: Sequence): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(sequence.getName)
      Option(sequence.getDescription).foreach(n => sb.append(" ").append(n))
      sequence.getSequence.grouped(lineWidth).foreach(line => {
        sb.append("\n")
        sb.append(line)
      })
      sb.append("\n")
      sb.toString
    }

    iter.foreach(sequence => os.write(toFasta(sequence).getBytes))
  }
}
