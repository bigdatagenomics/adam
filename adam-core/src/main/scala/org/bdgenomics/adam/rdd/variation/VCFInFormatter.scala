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
package org.bdgenomics.adam.rdd.variation

import htsjdk.variant.variantcontext.writer.{
  Options,
  VariantContextWriterBuilder
}
import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import java.io.OutputStream
import org.bdgenomics.adam.converters.{
  SupportedHeaderLines,
  VariantContextConverter
}
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.{ InFormatter, InFormatterCompanion }
import scala.collection.JavaConversions._

object VCFInFormatter extends InFormatterCompanion[VariantContext, VariantContextRDD, VCFInFormatter] {

  def apply(gRdd: VariantContextRDD): VCFInFormatter = {
    VCFInFormatter(gRdd.sequences, gRdd.samples.map(_.getSampleId))
  }
}

case class VCFInFormatter(sequences: SequenceDictionary,
                          samples: Seq[String]) extends InFormatter[VariantContext, VariantContextRDD, VCFInFormatter] {

  protected val companion = VCFInFormatter

  // make a converter
  val converter = new VariantContextConverter(Some(sequences))

  /**
   * Writes variant contexts to an output stream in VCF format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[VariantContext]) {

    // create a sam file writer connected to the output stream
    val writer = new VariantContextWriterBuilder()
      .setOutputStream(os)
      .clearIndexCreator()
      .unsetOption(Options.INDEX_ON_THE_FLY)
      .build()

    val headerLines: Set[VCFHeaderLine] = (SupportedHeaderLines.infoHeaderLines ++
      SupportedHeaderLines.formatHeaderLines).toSet
    val header = new VCFHeader(headerLines, samples)
    header.setSequenceDictionary(sequences.toSAMSequenceDictionary)
    writer.writeHeader(header)

    // write the records
    iter.foreach(r => {
      val vc = converter.convert(r)
      writer.add(vc)
    })

    // close the writer, else stream may be defective
    writer.close()
  }
}
