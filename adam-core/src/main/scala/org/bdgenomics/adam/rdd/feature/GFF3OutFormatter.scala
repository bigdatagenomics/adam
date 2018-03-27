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
package org.bdgenomics.adam.rdd.feature

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{
  AsciiLineReader,
  AsciiLineReaderIterator
}
import java.io.InputStream
import org.bdgenomics.adam.rdd.OutFormatter
import org.bdgenomics.formats.avro.Feature
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * OutFormatter that reads streaming GFF3 format.
 *
 * @param stringency Validation stringency. Defaults to ValidationStringency.STRICT.
 */
case class GFF3OutFormatter(
    val stringency: ValidationStringency = ValidationStringency.STRICT) extends OutFormatter[Feature] {

  val gff3Parser = new GFF3Parser

  /**
   * Reads features from an input stream in GFF3 format.
   *
   * @param is An InputStream connected to the process we are piping from.
   * @return Returns an iterator of Features read from the stream.
   */
  def read(is: InputStream): Iterator[Feature] = {

    // make line reader iterator
    val lri = new AsciiLineReaderIterator(new AsciiLineReader(is))

    @tailrec def convertIterator(iter: AsciiLineReaderIterator,
                                 features: ListBuffer[Feature] = ListBuffer.empty): Iterator[Feature] = {
      if (!iter.hasNext) {
        iter.close()
        features.toIterator
      } else {
        val nextFeatures = gff3Parser.parse(iter.next, stringency).fold(features)(features += _)
        convertIterator(iter, nextFeatures)
      }
    }

    // convert the iterator
    convertIterator(lri)
  }
}
