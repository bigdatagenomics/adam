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
package org.bdgenomics.adam.util

import java.io.{ BufferedWriter, OutputStreamWriter }
import htsjdk.samtools.SAMSequenceDictionaryCodec
import htsjdk.samtools.util.BufferedLineReader
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.SequenceDictionary

/**
 * Object for reading reference sequence dictionary files (.dict) from disk.
 */
object SequenceDictionaryReader {

  /**
   * Populates a reference SequenceDictionary from a .dict file on disk.
   *
   * @param filePath The path to read the dictionary from.
   * @param sc The SparkContext to use for configuration.
   * @return Returns a populated reference sequence dictionary.
   */
  def apply(filePath: String,
            sc: SparkContext): SequenceDictionary = {

    // get the file system
    val path = new Path(filePath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)

    // open the underlying file and wrap it up in a buffer for htsjdk
    val is = fs.open(path)
    val reader = new BufferedLineReader(is)

    // create a codec
    // even though we are just using the codec to read, it requires us to give
    // a writer...? setting the writer to stderr seems like a reasonable choice.
    val codec = new SAMSequenceDictionaryCodec(
      new BufferedWriter(new OutputStreamWriter(System.err)))

    // decode the dictionary; htsjdk allows a null source here
    val samDict = codec.decode(reader, null)

    // close the input source
    is.close()

    // create a sequence dictionary and return
    SequenceDictionary.fromSAMSequenceDictionary(samDict)
  }
}

