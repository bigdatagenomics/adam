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
package org.bdgenomics.adam.ds.read

import htsjdk.samtools._
import java.io.InputStream
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.ds.OutFormatter
import org.bdgenomics.formats.avro.Alignment
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * An OutFormatter that automatically infers whether the piped input is SAM or
 * BAM. Autodetecting streamed CRAM is not currently supported.
 */
case class AnySAMOutFormatter(stringency: ValidationStringency) extends OutFormatter[Alignment] {

  def this() = this(ValidationStringency.STRICT)

  /**
   * Reads alignments from an input stream. Autodetects SAM/BAM format.
   *
   * @param is An InputStream connected to a process we are piping from.
   * @return Returns an iterator of Alignments read from the stream.
   */
  def read(is: InputStream): Iterator[Alignment] = {

    // make reader
    val reader = SamReaderFactory.makeDefault()
      .validationStringency(stringency)
      .open(SamInputResource.of(is))

    SAMIteratorConverter(reader)
  }
}

private case class SAMIteratorConverter(val reader: SamReader) extends Iterator[Alignment] {

  val iter = reader.iterator()

  // make converter and empty dicts
  val converter = new SAMRecordConverter

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: Alignment = {
    assert(iter.hasNext)
    converter.convert(iter.next)
  }
}
