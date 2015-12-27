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
package org.bdgenomics.adam.apis.java

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.api.java.JavaRDD
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.formats.avro._

private class JavaSaveArgs(var outputPath: String,
                           var blockSize: Int = 128 * 1024 * 1024,
                           var pageSize: Int = 1 * 1024 * 1024,
                           var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                           var disableDictionaryEncoding: Boolean = false,
                           var asSingleFile: Boolean = false) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
}

class JavaAlignmentRecordRDD(val jrdd: JavaRDD[AlignmentRecord],
                             val sd: SequenceDictionary,
                             val rgd: RecordGroupDictionary) extends Serializable {

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   */
  def adamSave(
    filePath: java.lang.String,
    blockSize: java.lang.Integer,
    pageSize: java.lang.Integer,
    compressCodec: CompressionCodecName,
    disableDictionaryEncoding: java.lang.Boolean) {
    jrdd.rdd.saveAsParquet(
      new JavaSaveArgs(filePath,
        blockSize = blockSize,
        pageSize = pageSize,
        compressionCodec = compressCodec,
        disableDictionaryEncoding = disableDictionaryEncoding),
      sd,
      rgd)
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   */
  def adamSave(
    filePath: java.lang.String) {
    jrdd.rdd.saveAsParquet(
      new JavaSaveArgs(filePath),
      sd,
      rgd)
  }

  /**
   * Saves this RDD to disk as a SAM/BAM file.
   *
   * @param filePath Path to save the file at.
   * @param sd A dictionary describing the contigs this file is aligned against.
   * @param rgd A dictionary describing the read groups in this file.
   * @param asSam If true, saves as SAM. If false, saves as BAM.
   * @param asSingleFile If true, saves output as a single file.
   * @param isSorted If the output is sorted, this will modify the header.
   */
  def adamSAMSave(
    filePath: java.lang.String,
    asSam: java.lang.Boolean,
    asSingleFile: java.lang.Boolean,
    isSorted: java.lang.Boolean) {
    jrdd.rdd.adamSAMSave(filePath,
      sd,
      rgd,
      asSam = asSam,
      asSingleFile = asSingleFile,
      isSorted = isSorted)
  }
}
