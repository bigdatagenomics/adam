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

import java.io._
import org.bdgenomics.adam.util.ADAMFunSuite

class PluginExecutorSuite extends ADAMFunSuite {

  sparkTest("take10 works correctly on example SAM") {

    val args = new PluginExecutorArgs()
    args.plugin = "org.bdgenomics.adam.plugins.Take10Plugin"
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("reads12.sam")
    val file = File.createTempFile("reads12", ".sam")
    val os = new FileOutputStream(file)
    val bytes = new Array[Byte](stream.available())
    stream.read(bytes)
    os.write(bytes)
    args.input = file.getAbsolutePath

    val pluginExecutor = new PluginExecutor(args)

    val bytesWritten = new ByteArrayOutputStream()
    scala.Console.withOut(bytesWritten)(pluginExecutor.run(sc))

    val outputString = bytesWritten.toString

    // Make sure that 10 lines are output
    assert(outputString.lines.size === 10)
  }

  sparkTest("java take10 works correctly on example SAM") {

    val args = new PluginExecutorArgs()
    args.plugin = "org.bdgenomics.adam.apis.java.JavaTake10Plugin"
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("reads12.sam")
    val file = File.createTempFile("reads12", ".sam")
    val os = new FileOutputStream(file)
    val bytes = new Array[Byte](stream.available())
    stream.read(bytes)
    os.write(bytes)
    args.input = file.getAbsolutePath

    val pluginExecutor = new PluginExecutor(args)

    val bytesWritten = new ByteArrayOutputStream()
    scala.Console.withOut(bytesWritten)(pluginExecutor.run(sc))

    val outputString = bytesWritten.toString

    // Make sure that 10 lines are output
    assert(outputString.lines.size === 10)
  }

  sparkTest("takeN works correctly on example SAM with arg of '3'") {

    val args = new PluginExecutorArgs()
    args.plugin = "org.bdgenomics.adam.plugins.TakeNPlugin"
    args.pluginArgs = "3"

    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("reads12.sam")
    val file = File.createTempFile("reads12", ".sam")
    val os = new FileOutputStream(file)
    val bytes = new Array[Byte](stream.available())
    stream.read(bytes)
    os.write(bytes)
    args.input = file.getAbsolutePath

    val pluginExecutor = new PluginExecutor(args)

    val bytesWritten = new ByteArrayOutputStream()
    scala.Console.withOut(bytesWritten)(pluginExecutor.run(sc))

    val outputString = bytesWritten.toString

    // Make sure that 3 lines are output
    assert(outputString.lines.size === 3)
  }
}
