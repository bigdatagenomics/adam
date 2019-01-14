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
package org.bdgenomics.adam.rdd.read

import htsjdk.samtools.{
  SAMFileWriter,
  SAMFileWriterFactory
}
import java.io.OutputStream
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.{
  ReadGroupDictionary,
  SAMFileHeaderWritable
}

/**
 * InFormatter companion for building an InFormatter that streams SAM.
 */
object SAMInFormatter extends AnySAMInFormatterCompanion[SAMInFormatter] {

  protected def makeFormatter(header: SAMFileHeaderWritable,
                              readGroups: ReadGroupDictionary,
                              converter: AlignmentRecordConverter): SAMInFormatter = {
    SAMInFormatter(header, readGroups, converter)
  }
}

case class SAMInFormatter private (
    header: SAMFileHeaderWritable,
    readGroups: ReadGroupDictionary,
    converter: AlignmentRecordConverter) extends AnySAMInFormatter[SAMInFormatter] {

  def this() = {
    this(null, null, null)
  }

  protected val companion = SAMInFormatter

  protected def makeWriter(os: OutputStream): SAMFileWriter = {
    new SAMFileWriterFactory()
      .makeSAMWriter(header.header, true, os)
  }
}
