/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.broadinstitute.variant.vcf.{ VCFHeaderLine, VCFHeader }
import org.bdgenomics.adam.converters.VariantAnnotationConverter
import fi.tkk.ics.hadoop.bam.{ VCFFormat, KeyIgnoringVCFOutputFormat }
import scala.collection.JavaConversions._

object ADAMVCFOutputFormat {
  private var header: Option[VCFHeader] = None

  def getHeader: VCFHeader = header match {
    case Some(h) => h
    case None    => setHeader(Seq())
  }

  def setHeader(samples: Seq[String]): VCFHeader = {
    header = Some(new VCFHeader(
      (VariantAnnotationConverter.infoHeaderLines ++ VariantAnnotationConverter.formatHeaderLines).toSet: Set[VCFHeaderLine],
      samples))
    header.get
  }
}

/**
 * Wrapper for Hadoop-BAM to work around requirement for no-args constructor. Depends on
 * ADAMVCFOutputFormat object to maintain global state (such as samples)
 *
 * @tparam K
 */
class ADAMVCFOutputFormat[K] extends KeyIgnoringVCFOutputFormat[K](VCFFormat.VCF) {
  setHeader(ADAMVCFOutputFormat.getHeader)
}