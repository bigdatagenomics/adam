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

import java.nio.file.Files

import org.bdgenomics.utils.misc.SparkFunSuite

import scala.io.Source

trait ADAMFunSuite extends SparkFunSuite {

  override val appName: String = "adam"
  override val properties: Map[String, String] = Map(
    ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
    ("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator"),
    ("spark.kryo.referenceTracking", "true"))

  def resourcePath(path: String) = ClassLoader.getSystemClassLoader.getResource(path).getFile
  def tmpFile(path: String) = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

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
}

