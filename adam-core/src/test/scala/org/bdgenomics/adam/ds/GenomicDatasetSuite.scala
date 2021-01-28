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
package org.bdgenomics.adam.ds

import java.nio.file.Paths
import org.bdgenomics.adam.util.ADAMFunSuite

class GenomicDatasetSuite extends ADAMFunSuite {

  sparkTest("processing a command that is the spark root directory should return an absolute path") {
    val cmd = GenomicDataset.processCommand(Seq("$root"), Seq.empty)

    assert(cmd.size === 1)
    val path = Paths.get(cmd.head)
    assert(path.isAbsolute())
  }

  sparkTest("processing a command that is just a single word should do nothing") {
    val cmd = GenomicDataset.processCommand(Seq("ls"), Seq.empty)

    assert(cmd.size === 1)
    assert(cmd.head === "ls")
  }

  sparkTest("processing a command should handle arguments that include spaces") {
    val cmd = GenomicDataset.processCommand(Seq("foo",
      "--arg", "value-with-no-spaces",
      "--arg-with-space", "value includes spaces"), Seq.empty)

    assert(cmd.size === 5)
    assert(cmd.head === "foo")
    assert(cmd(2) == "value-with-no-spaces")
    assert(cmd(4) === "value includes spaces")
  }

  sparkTest("processing a command that is a single substitution should succeed") {
    val cmd = GenomicDataset.processCommand(Seq("$0"), Seq("/bin/bash"))

    assert(cmd.size === 1)
    assert(cmd.head === "/bin/bash")
  }

  sparkTest("processing a command that is multiple words should split the string") {
    val cmd = GenomicDataset.processCommand(Seq("tee", "/dev/null"), Seq.empty)

    assert(cmd.size === 2)
    assert(cmd(0) === "tee")
    assert(cmd(1) === "/dev/null")
  }

  sparkTest("process a command that is multiple words with a replacement") {
    val cmd = GenomicDataset.processCommand(Seq("echo", "$0"), Seq("/path/to/my/file"))

    assert(cmd.size === 2)
    assert(cmd(0) === "echo")
    assert(cmd(1) === "/path/to/my/file")
  }

  sparkTest("process a command that is multiple words with multiple replacements") {
    val cmd = GenomicDataset.processCommand(Seq("aCommand", "$0", "hello", "$1"), Seq("/path/to/my/file",
      "/path/to/another/file"))

    assert(cmd.size === 4)
    assert(cmd(0) === "aCommand")
    assert(cmd(1) === "/path/to/my/file")
    assert(cmd(2) === "hello")
    assert(cmd(3) === "/path/to/another/file")
  }
}
