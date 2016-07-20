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
  SAMFileReader,
  SAMRecord,
  SAMRecordIterator
}
import java.io.InputStream
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.OutFormatter
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class AnySAMOutFormatter extends OutFormatter[AlignmentRecord] {

  /**
   * Reads alignment records from an input stream. Autodetects SAM/BAM format.
   *
   * @param is An InputStream connected to a process we are piping from.
   * @return Returns an iterator of AlignmentRecords read from the stream.
   */
  def read(is: InputStream): Iterator[AlignmentRecord] = {

    // make converter and empty dicts
    val converter = new SAMRecordConverter

    // make reader
    val reader = new SAMFileReader(is)

    // make iterator from said reader
    val iter = reader.iterator()

    @tailrec def convertIterator(iter: SAMRecordIterator,
                                 records: ListBuffer[AlignmentRecord] = ListBuffer.empty): Iterator[AlignmentRecord] = {
      if (!iter.hasNext) {
        iter.close()
        records.toIterator
      } else {
        val nextRecords = records += converter.convert(iter.next)
        convertIterator(iter, nextRecords)
      }
    }

    // convert the iterator
    convertIterator(iter)
  }
}
