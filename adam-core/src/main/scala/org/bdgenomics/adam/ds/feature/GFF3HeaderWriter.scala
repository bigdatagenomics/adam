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
package org.bdgenomics.adam.ds.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

/**
 * Writes the header for a GFF3 file to an otherwise empty file.
 */
private[feature] object GFF3HeaderWriter {

  val HEADER_STRING = "##gff-version 3.2.1"

  /**
   * Writes a GFF3 Header pragma to a file.
   *
   * @param filePath The path to write the file to.
   * @param sc The SparkContext, to access the Hadoop FS Configuration.
   */
  def apply(filePath: String,
            sc: SparkContext) {
    val path = new Path(filePath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val os = fs.create(path)
    os.write(HEADER_STRING.getBytes)
    os.write("\n".getBytes)
    os.close()
  }
}
