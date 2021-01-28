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
package org.bdgenomics.adam.ds.feature

import java.io.{
  BufferedWriter,
  OutputStream,
  OutputStreamWriter
}
import org.bdgenomics.adam.ds.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.adam.sql.{ Feature => FeatureProduct }
import org.bdgenomics.formats.avro.Feature

/**
 * InFormatter companion that builds a GTFInFormatter to write features in GTF format to a pipe.
 */
object GTFInFormatter extends InFormatterCompanion[Feature, FeatureProduct, FeatureDataset, GTFInFormatter] {

  /**
   * Apply method for building the GTFInFormatter from a FeatureDataset.
   *
   * @param fRdd FeatureDataset to build from.
   */
  def apply(fRdd: FeatureDataset): GTFInFormatter = {
    GTFInFormatter()
  }
}

case class GTFInFormatter private () extends InFormatter[Feature, FeatureProduct, FeatureDataset, GTFInFormatter] {
  protected val companion = GTFInFormatter

  /**
   * Writes features to an output stream in GTF format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of features to write.
   */
  def write(os: OutputStream, iter: Iterator[Feature]) {
    val writer = new BufferedWriter(new OutputStreamWriter(os))

    // write the features
    iter.foreach(f => {
      writer.write(FeatureDataset.toGtf(f))
      writer.newLine()
    })

    // close the writer, else stream may be defective
    writer.close() // os is flushed and closed in InFormatterRunner
  }
}
