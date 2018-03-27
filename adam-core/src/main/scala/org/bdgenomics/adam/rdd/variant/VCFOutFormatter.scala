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

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.{
  VCFCodec,
  VCFHeader,
  VCFHeaderLine
}
import htsjdk.tribble.readers.{
  AsciiLineReader,
  AsciiLineReaderIterator
}
import java.io.InputStream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.CollectionAccumulator
import org.bdgenomics.adam.converters.VariantContextConverter._
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.OutFormatter
import org.bdgenomics.utils.misc.Logging
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * OutFormatter that reads streaming VCF.
 *
 * @param conf Hadoop configuration.
 * @param stringency Validation stringency.
 * @param optHeaderLines Optional accumulator for VCF header lines.
 */
case class VCFOutFormatter(
    @transient conf: Configuration,
    val stringency: ValidationStringency,
    val optHeaderLines: Option[CollectionAccumulator[VCFHeaderLine]]) extends OutFormatter[VariantContext] with Logging {

  private val nestAnn = VariantContextConverter.getNestAnnotationInGenotypesProperty(conf)

  /**
   * OutFormatter that reads streaming VCF. Defaults to ValidationStringency.LENIENT.
   * Java-friendly no-arg constructor.
   *
   * @param conf Hadoop configuration.
   */
  def this(conf: Configuration) = this(conf, ValidationStringency.LENIENT, None)

  /**
   * OutFormatter that reads streaming VCF. Java-friendly constructor.
   *
   * @param conf Hadoop configuration.
   * @param acc Accumulator for VCF header lines.
   */
  def this(conf: Configuration, stringency: ValidationStringency) = this(conf, stringency, None)

  /**
   * OutFormatter that reads streaming VCF. Defaults to ValidationStringency.LENIENT.
   * Java-friendly constructor.
   *
   * @param conf Hadoop configuration.
   * @param acc Accumulator for VCF header lines.
   */
  def this(conf: Configuration, acc: CollectionAccumulator[VCFHeaderLine]) = this(conf, ValidationStringency.LENIENT, Some(acc))

  /**
   * OutFormatter that reads streaming VCF. Defaults to ValidationStringency.LENIENT.
   * Java-friendly constructor.
   *
   * @param conf Hadoop configuration.
   * @param optHeaderLines Optional accumulator for VCF header lines.
   */
  def this(conf: Configuration, optHeaderLines: Option[CollectionAccumulator[VCFHeaderLine]]) = this(conf, ValidationStringency.LENIENT, optHeaderLines)

  /**
   * Reads VariantContexts from an input stream. Autodetects VCF format.
   *
   * @param is An InputStream connected to a process we are piping from.
   * @return Returns an iterator of AlignmentRecords read from the stream.
   */
  def read(is: InputStream): Iterator[VariantContext] = {

    // make line reader iterator
    val lri = new AsciiLineReaderIterator(new AsciiLineReader(is))

    // make reader
    val codec = new VCFCodec()

    // read the header
    val header = codec.readActualHeader(lri).asInstanceOf[VCFHeader]

    // merge header lines with our supported header lines
    val lines = cleanAndMixInSupportedLines(headerLines(header), stringency, log)

    // accumulate header lines if desired
    optHeaderLines.map(accumulator => lines.foreach(line => accumulator.add(line)))

    // make converter
    val converter = new VariantContextConverter(lines,
      stringency,
      nestAnn)

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
