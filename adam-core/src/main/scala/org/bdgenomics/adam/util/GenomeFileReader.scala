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

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.{ SequenceDictionary, SequenceRecord }

/**
 * Object for reading Bedtools genome files from disk. Also supports
 * UCSC Genome Browser chromInfo files.
 */
object GenomeFileReader {

  /**
   * Populates a SequenceDictionary from a .genome file on disk.
   *
   * @param filePath The path to read the dictionary from.
   * @param sc The SparkContext to use for configuration.
   * @return Returns a populated sequence dictionary.
   */
  def apply(filePath: String,
            sc: SparkContext): SequenceDictionary = {

    val records = sc
      .textFile(filePath)
      .map(line => line.split("\t"))
      .map(tokens => if (tokens.length > 2) {
        SequenceRecord(
          tokens(0),
          tokens(1).toLong,
          url = Some(tokens(2)),
          md5 = None,
          refseq = None,
          genbank = None,
          assembly = None,
          species = None,
          index = None
        )
      } else {
        SequenceRecord(tokens(0), tokens(1).toLong)
      })
      .collect

    new SequenceDictionary(records.toVector)
  }
}
