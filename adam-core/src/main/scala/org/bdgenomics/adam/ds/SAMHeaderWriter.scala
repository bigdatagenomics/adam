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

import htsjdk.samtools.{ SAMFileHeader, SAMTextWriter, SAMTextHeaderCodec }
import java.io.{ OutputStream, StringWriter, Writer }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.bdgenomics.adam.models.SequenceDictionary

/**
 * Helper object for writing a SAM header.
 */
private[ds] object SAMHeaderWriter {

  /**
   * Writes a header containing just reference contig information.
   *
   * @param fs The underlying file system to write to.
   * @param path The path to write the header at.
   * @param sequences The sequence dictionary to write.
   */
  def writeHeader(fs: FileSystem,
                  path: Path,
                  sequences: SequenceDictionary) {

    // create a header and attach a sequence dictionary
    val header = new SAMFileHeader
    header.setSequenceDictionary(sequences.toSAMSequenceDictionary)

    // call to our friend
    writeHeader(fs, path, header)
  }

  /**
   * Writes a full SAM header to a filesystem.
   *
   * @param fs The underlying file system to write to.
   * @param path The path to write the header at.
   * @param header The header to write.
   */
  def writeHeader(fs: FileSystem,
                  path: Path,
                  header: SAMFileHeader) {

    // get an output stream
    val os = fs.create(path)
      .asInstanceOf[OutputStream]

    // get a string writer and set up the text writer
    val sw: Writer = new StringWriter()
    val stw = new SAMTextWriter(os)
    val sthc = new SAMTextHeaderCodec()
    sthc.encode(sw, header)

    // write header to file
    stw.writeHeader(sw.toString)
    stw.close()

    // flush and close stream
    os.flush()
    os.close()
  }
}
