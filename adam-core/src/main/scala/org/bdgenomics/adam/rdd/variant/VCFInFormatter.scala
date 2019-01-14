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
import htsjdk.variant.variantcontext.writer.{
  Options,
  VariantContextWriterBuilder
}
import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import java.io.OutputStream
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.{ InFormatter, InFormatterCompanion }
import org.bdgenomics.adam.sql.{ VariantContext => VariantContextProduct }
import scala.collection.JavaConversions._

/**
 * InFormatter companion that builds a VCFInFormatter to write VCF to a pipe.
 */
object VCFInFormatter extends InFormatterCompanion[VariantContext, VariantContextProduct, VariantContextDataset, VCFInFormatter] {

  /**
   * Apply method for building the VCFInFormatter from a VariantContextDataset.
   *
   * @param gDataset VariantContextDataset to build VCF header from.
   * @return A constructed VCFInFormatter with all needed metadata to write a
   *   VCF header.
   */
  def apply(gDataset: VariantContextDataset): VCFInFormatter = {
    VCFInFormatter(gDataset.sequences,
      gDataset.samples.map(_.getId),
      gDataset.headerLines,
      gDataset.rdd.context.hadoopConfiguration)
  }
}

case class VCFInFormatter private (
    sequences: SequenceDictionary,
    samples: Seq[String],
    headerLines: Seq[VCFHeaderLine],
    @transient val conf: Configuration) extends InFormatter[VariantContext, VariantContextProduct, VariantContextDataset, VCFInFormatter] {

  protected val companion = VCFInFormatter

  // make a converter
  val converter = VariantContextConverter(headerLines,
    ValidationStringency.LENIENT,
    conf)

  /**
   * Writes variant contexts to an output stream in VCF format.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[VariantContext]) {

    // create a vcf writer connected to the output stream
    val writer = new VariantContextWriterBuilder()
      .setOutputStream(os)
      .clearIndexCreator()
      .unsetOption(Options.INDEX_ON_THE_FLY)
      .build()

    val header = new VCFHeader(headerLines.toSet, samples)
    header.setSequenceDictionary(sequences.toSAMSequenceDictionary)
    writer.writeHeader(header)

    // write the records
    iter.foreach(r => {
      val optVc = converter.convert(r)
      optVc.foreach(vc => {
        writer.add(vc)
      })
    })

    // close the writer, else stream may be defective
    writer.close()
  }
}
