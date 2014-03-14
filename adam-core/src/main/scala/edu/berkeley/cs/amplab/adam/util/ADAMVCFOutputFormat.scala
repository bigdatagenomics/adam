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
package edu.berkeley.cs.amplab.adam.util

import fi.tkk.ics.hadoop.bam.{KeyIgnoringVCFOutputFormat, VCFFormat}
import org.broadinstitute.variant.vcf.VCFHeader

/**
 * A static object for attaching a header to the accompanying ADAMVCFOutputFormat hadoop writer.
 * Categorically, this static attachment method is not good, and makes me feel unclean. Alack, there
 * appears to be no workaround. Hadoop-BAM will not write a VCF to disk unless a header is attached,
 * and Hadoop-BAM itself does not have a static way of attaching a header that works with the Spark
 * Hadoop API writer.
 */
object ADAMVCFOutputFormat {

  private[util] var header: Option[VCFHeader] = None

  /**
   * Attaches a header to the ADAMVCFOutputFormat Hadoop writer. If a header has previously
   * been attached, the header must be cleared first.
   *
   * @throws Exception Exception thrown if a VCF header has previously been attached, and not cleared.
   * 
   * @param vcfHeader Header to attach.
   *
   * @see clearHeader
   */
  def addHeader (vcfHeader: VCFHeader) {
    assert(header.isEmpty, "Cannot attach a new VCF header without first clearing the header.")
    header = Some(vcfHeader)
  }

  /**
   * Clears the attached header.
   *
   * @see addHeader
   */
  def clearHeader() {
    header = None
  }

  /**
   * Returns the current header.
   *
   * @return Current VCF header.
   */
  private[util] def getHeader(): VCFHeader = {
    assert(header.isDefined, "Cannot return header if not attached.")
    header.get
  }
}

class ADAMVCFOutputFormat[K]
  extends KeyIgnoringVCFOutputFormat[K](VCFFormat.valueOf("VCF")) {

  setHeader(ADAMVCFOutputFormat.getHeader)
}

