/**
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.cli

import java.io._
import org.scalatest.FunSuite

class PluginExecutorSuite extends FunSuite {

  test("take10 works correctly on example SAM") {

    val args = new PluginExecutorArgs()
    args.plugin = "edu.berkeley.cs.amplab.adam.plugins.Take10Plugin"
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("reads12.sam")
    val file = File.createTempFile("reads12", ".sam")
    val os = new FileOutputStream(file)
    val bytes = new Array[Byte](stream.available())
    stream.read(bytes)
    os.write(bytes)
    args.input = file.getAbsolutePath

    val pluginExecutor = new PluginExecutor(args)

    val bytesWritten = new ByteArrayOutputStream()
    scala.Console.withOut(bytesWritten)(pluginExecutor.run())

    val outputString = bytesWritten.toString

    // Make sure that 10 lines are output
    assert(outputString.lines.size === 10)
  }
}
