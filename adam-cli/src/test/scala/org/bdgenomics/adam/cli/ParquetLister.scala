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
package org.bdgenomics.adam.cli

import java.io.File

import grizzled.slf4j.Logging
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetReader

import scala.reflect.ClassTag

class ParquetLister[T <: IndexedRecord](projectionSchema: Option[Schema] = None)(implicit classTag: ClassTag[T])
    extends Logging {
  /**
   * Given a full path to the local filesystem, checks whether the path is a file --
   * in which case, it materializes the row groups from that file -- or if it's a directory.
   * If the path is a directory, it lists the row groups of all the files within the directory.
   *
   * @param fullPath The path on the local filesystem, corresponding either to a Parquet file
   *                 or a directory filled with parquet files.
   * @return An iterator over the values requested from the file (or, in case fullPath is a directory,
   *         _all_ the files in some arbitrary order)
   */
  def materialize(fullPath: String): Iterator[T] = {
    val file = new File(fullPath)
    val relativePath = file.getName
    if (file.isFile) {
      materialize(file)
    } else {
      val childFiles = file.listFiles().filter(f => f.isFile && !f.getName.startsWith(".") && f.getName != "_SUCCESS")
      childFiles.flatMap {
        case f =>
          try {
            materialize(f)
          } catch {
            case e: IllegalArgumentException =>
              info("File %s doesn't appear to be a Parquet file; skipping".format(f))
              Seq()
          }
      }.iterator
    }
  }

  private def materialize(file: File): Iterator[T] = {
    info("Materializing file %s".format(file))
    val conf = new Configuration
    if (projectionSchema.isDefined) {
      AvroReadSupport.setRequestedProjection(conf, projectionSchema.get)
    }
    val reader = ParquetReader.builder(new AvroReadSupport[T], new Path(file.getAbsolutePath))
      .withConf(conf).build();
    Iterator.continually(reader.read()).takeWhile(_ != null)
  }
}
