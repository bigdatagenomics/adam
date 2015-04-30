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

import org.apache.hadoop.fs.{ FileSystem, Path }
import parquet.avro.AvroParquetReader
import org.apache.avro.generic.IndexedRecord
import org.apache.spark.SparkContext
import org.bdgenomics.utils.misc.HadoopUtil

class ParquetFileTraversable[T <: IndexedRecord](sc: SparkContext, file: Path) extends Traversable[T] {
  def this(sc: SparkContext, file: String) = this(sc, new Path(file))

  private val fs = FileSystem.get(sc.hadoopConfiguration)

  val paths: List[Path] = {
    if (!fs.exists(file)) {
      throw new IllegalArgumentException("The path %s does not exist".format(file))
    }
    val status = fs.getFileStatus(file)
    var paths = List[Path]()
    if (HadoopUtil.isDirectory(status)) {
      val files = fs.listStatus(file)
      files.foreach {
        file =>
          if (file.getPath.getName.contains("part")) {
            paths ::= file.getPath
          }
      }
    } else if (fs.isFile(file)) {
      paths ::= file
    } else {
      throw new IllegalArgumentException("The path '%s' is neither file nor directory".format(file))
    }
    paths
  }

  override def foreach[U](f: (T) => U) {
    for (path <- paths) {
      val parquetReader = new AvroParquetReader[T](path)
      var record = null.asInstanceOf[T]
      do {
        record = parquetReader.read()
        if (record != null.asInstanceOf[T]) {
          f(record)
        }
      } while (record != null.asInstanceOf[T])
      parquetReader.close()
    }
  }

}

