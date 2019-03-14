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

import java.io.File
import htsjdk.samtools.util.BlockCompressedOutputStream
import htsjdk.variant.variantcontext.writer.{
  Options,
  VariantContextWriterBuilder
}
import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Utility for writing VCF headers to a file.
 */
private[ds] object VCFHeaderUtils {

  /**
   * Writes a vcf header to a file.
   *
   * @param header The header to write.
   * @param path The path to write it to.
   * @param conf The configuration to get the file system.
   * @param isCompressed If true, writes as BGZIP.
   * @param noEof If writing a compressed file, omits the terminating BGZF EOF
   *   block. Ignored if output is uncompressed.
   */
  def write(header: VCFHeader,
            path: Path,
            conf: Configuration,
            isCompressed: Boolean,
            noEof: Boolean) {

    val fs = path.getFileSystem(conf)

    write(header, path, fs, isCompressed, noEof)
  }

  /**
   * Writes a vcf header to a file.
   *
   * @param header The header to write.
   * @param path The path to write it to.
   * @param fs The file system to write to.
   * @param isCompressed If true, writes as BGZIP.
   * @param noEof If writing a compressed file, omits the terminating BGZF EOF
   *   block. Ignored if output is uncompressed.
   */
  def write(header: VCFHeader,
            path: Path,
            fs: FileSystem,
            isCompressed: Boolean,
            noEof: Boolean) {

    // get an output stream
    val fos = fs.create(path)
    val os = if (isCompressed) {
      // BGZF stream requires java.io.File
      // we can't get one in hadoop land, so it is OK to provide a null file
      new BlockCompressedOutputStream(fos, null.asInstanceOf[File])
    } else {
      fos
    }

    // build a vcw
    val vcw = new VariantContextWriterBuilder()
      .setOutputVCFStream(os)
      .clearIndexCreator()
      .unsetOption(Options.INDEX_ON_THE_FLY)
      .build()

    // write the header
    vcw.writeHeader(header)

    // close the writer
    // originally, this was vcw.close, which calls close on the underlying
    // stream (see ADAM-1337 for a longer history). however, to support BGZF, we
    // need to separate out the calls. specifically, calling close on the vcw
    // calls close on the BlockCompressOutputStream, which leads to the stream
    // writing the BGZF EOF indicator (an empty BGZF block).
    //
    // if we are writing an uncompressed VCF or a sharded BGZF'ed VCF, this is
    // OK. if we write the BGZF EOF indicator at the end of the BGZF'ed VCF's
    // header and we are writing a merged BGZF'ed VCF, this leads to the reader
    // seeing an EOF at the end of the header and reading no records from the
    // vcf. if we call flush and close seperately, this behavior doesn't occur.
    if (isCompressed && noEof) {
      os.flush()
      fos.close()
    } else {
      vcw.close()
    }
  }
}
