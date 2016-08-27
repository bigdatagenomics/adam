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

import com.google.common.io.Resources
import java.io.File
import java.net.{ URI, URL }
import java.nio.file.{ Files, Path }

import org.bdgenomics.utils.misc.SparkFunSuite

import scala.io.Source

trait ADAMFunSuite extends SparkFunSuite {

  override val appName: String = "adam"
  override val properties: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.bdgenomics.adam.serialization.ADAMKryoRegistrator",
    "spark.kryo.referenceTracking" -> "true",
    "spark.kryo.registrationRequired" -> "true"
  )

  def resourceUrl(path: String): URL = ClassLoader.getSystemClassLoader.getResource(path)

  def tmpFile(path: String): String = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

  /**
   * Creates temporary file for saveAsParquet().
   * This helper function is required because createTempFile creates an actual file, not
   * just a name.  Whereas, this returns the name of something that could be mkdir'ed, in the
   * same location as createTempFile() uses, so therefore the returned path from this method
   * should be suitable for saveAsParquet().
   *
   * @param suffix Suffix of file being created
   * @return Absolute path to temporary file location
   */
  def tmpLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("TempSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  def checkFiles(expectedPath: String, actualPath: String): Unit = {
    val actualFile = Source.fromFile(actualPath)
    val actualLines = actualFile.getLines.toList

    val expectedFile = Source.fromFile(expectedPath)
    val expectedLines = expectedFile.getLines.toList

    assert(expectedLines.size === actualLines.size)
    expectedLines.zip(actualLines).zipWithIndex.foreach {
      case ((expected, actual), idx) =>
        assert(
          expected == actual,
          s"Line ${idx + 1} differs.\nExpected:\n${expectedLines.mkString("\n")}\n\nActual:\n${actualLines.mkString("\n")}"
        )
    }
  }

  def copyResourcePath(name: String): Path = {
    val tempFile = Files.createTempFile(name, "." + name.split('.').tail.mkString("."))
    Files.write(tempFile, Resources.toByteArray(getClass().getResource("/" + name)))
  }

  def copyResource(name: String): String = {
    copyResourcePath(name).toString
  }

  def copyResourceUri(name: String): URI = {
    copyResourcePath(name).toUri
  }
}

