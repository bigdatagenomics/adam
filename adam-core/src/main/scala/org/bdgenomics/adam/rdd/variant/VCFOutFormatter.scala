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
package org.bdgenomics.adam.rdd.variant

import htsjdk.variant.vcf.VCFCodec
import htsjdk.tribble.readers.{
  AsciiLineReader,
  AsciiLineReaderIterator
}
import java.io.InputStream
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.OutFormatter
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * OutFormatter that reads streaming VCF.
 */
case class VCFOutFormatter() extends OutFormatter[VariantContext] {

  /**
   * Reads VariantContexts from an input stream. Autodetects VCF format.
   *
   * @param is An InputStream connected to a process we are piping from.
   * @return Returns an iterator of AlignmentRecords read from the stream.
   */
  def read(is: InputStream): Iterator[VariantContext] = {

    // make converter and empty dicts
    val converter = new VariantContextConverter

    // make line reader iterator
    val lri = new AsciiLineReaderIterator(new AsciiLineReader(is))

    // make reader
    val codec = new VCFCodec()

    // read the header, and then throw it away
    val header = codec.readActualHeader(lri)

    @tailrec def convertIterator(iter: AsciiLineReaderIterator,
                                 records: ListBuffer[VariantContext] = ListBuffer.empty): Iterator[VariantContext] = {
      if (!iter.hasNext) {
        iter.close()
        records.toIterator
      } else {
        val nextRecords = records ++ converter.convert(codec.decode(iter.next))
        convertIterator(iter, nextRecords)
      }
    }

    // convert the iterator
    convertIterator(lri)
  }
}
