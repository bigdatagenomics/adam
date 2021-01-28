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
package org.bdgenomics.adam.ds

import java.io.OutputStream

private[ds] class InFormatterRunner[T, U <: Product, V <: GenomicDataset[T, U, V], W <: InFormatter[T, U, V, W]](iter: Iterator[T],
                                                                                                                 formatter: W,
                                                                                                                 os: OutputStream) extends Runnable {

  def run() {
    formatter.write(os, iter)
    os.flush()
    os.close()
  }
}

/**
 * A trait for singleton objects that build an InFormatter from a GenomicDataset.
 *
 * Often, when creating an outputstream, we need to add metadata to the output
 * that is not attached to individual records. An example of this is writing a
 * header with contig/read group/format info, as is done with SAM/BAM/VCF.
 *
 * @tparam T The type of the records this InFormatter writes out.
 * @tparam U The type of the GenomicDataset this companion object understands.
 * @tparam V The type of InFormatter this companion object creates.
 */
trait InFormatterCompanion[T, U <: Product, V <: GenomicDataset[T, U, V], W <: InFormatter[T, U, V, W]] {

  /**
   * Creates an InFormatter from a GenomicDataset.
   *
   * @param gDataset The GenomicDataset to get metadata from.
   * @return Returns an InFormatter with attached metadata.
   */
  def apply(gDataset: V): W
}

/**
 * Formats data going into a pipe to an invoked process.
 *
 * @tparam T The type of records being formatted.
 */
trait InFormatter[T, U <: Product, V <: GenomicDataset[T, U, V], W <: InFormatter[T, U, V, W]] extends Serializable {

  protected val companion: InFormatterCompanion[T, U, V, W]

  /**
   * Writes records from an iterator into an output stream.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[T])
}

