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
package org.bdgenomics.adam.ds.read

import java.io.OutputStream
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.ds.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.adam.sql.{ Alignment => AlignmentProduct }
import org.bdgenomics.formats.avro.Alignment

/**
 * InFormatter companion that creates an InFormatter that writes FASTQ.
 */
object FASTQInFormatter extends InFormatterCompanion[Alignment, AlignmentProduct, AlignmentDataset, FASTQInFormatter] {

  /**
   * Builds an FASTQInFormatter to write FASTQ.
   *
   * @param gDataset GenomicDataset of Alignments. Used to get HadoopConfiguration.
   * @return Returns a new Single FASTQ InFormatter.
   */
  def apply(gDataset: AlignmentDataset): FASTQInFormatter = {
    new FASTQInFormatter(gDataset.rdd.context.hadoopConfiguration)
  }
}

class FASTQInFormatter private (
    conf: Configuration) extends InFormatter[Alignment, AlignmentProduct, AlignmentDataset, FASTQInFormatter] {

  protected val companion = FASTQInFormatter
  private val converter = new AlignmentConverter
  private val writeSuffixes = conf.getBoolean(AlignmentDataset.WRITE_SUFFIXES, false)
  private val writeOriginalQualityScores = conf.getBoolean(AlignmentDataset.WRITE_ORIGINAL_QUALITY_SCORES, false)

  /**
   * Writes alignments to an output stream in FASTQ format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[Alignment]) {
    iter.foreach(read => {
      val fastqRead = converter.convertToFastq(read,
        maybeAddSuffix = writeSuffixes,
        writeOriginalQualityScores) + "\n"

      os.write(fastqRead.getBytes)
    })
  }
}
