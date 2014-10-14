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

import org.apache.spark.api.java.JavaRDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordContext._
import org.bdgenomics.formats.avro._
import parquet.hadoop.metadata.CompressionCodecName

class JavaAlignmentRecordRDD(val jrdd: JavaRDD[AlignmentRecord]) extends Serializable {

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   */
  def adamSave(filePath: java.lang.String,
               blockSize: java.lang.Integer,
               pageSize: java.lang.Integer,
               compressCodec: CompressionCodecName,
               disableDictionaryEncoding: java.lang.Boolean) {
    jrdd.rdd.adamSave(filePath,
      blockSize,
      pageSize,
      compressCodec,
      disableDictionaryEncoding)
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   */
  def adamSave(filePath: java.lang.String) {
    jrdd.rdd.adamSave(filePath)
  }

  /**
   * Saves this RDD to disk as a SAM/BAM file.
   *
   * @param filePath Path to save the file at.
   * @param asSam If true, saves as SAM. If false, saves as BAM.
   */
  def adamSAMSave(filePath: java.lang.String,
                  asSam: java.lang.Boolean) {
    jrdd.rdd.adamSAMSave(filePath, asSam)
  }

  /**
   * Saves this RDD to disk as a SAM/BAM file.
   *
   * @param filePath Path to save the file at.
   */
  def adamSAMSave(filePath: java.lang.String) {
    jrdd.rdd.adamSAMSave(filePath)
  }
}
