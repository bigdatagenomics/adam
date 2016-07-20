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
package org.bdgenomics.adam.rdd

import org.scalatest.FunSuite

class GenomicRDDSuite extends FunSuite {

  test("processing a command that is just a single word should do nothing") {
    val cmd = GenomicRDD.processCommand("ls", Seq.empty)

    assert(cmd.size === 1)
    assert(cmd.head === "ls")
  }

  test("processing a command that is a single substitution should succeed") {
    val cmd = GenomicRDD.processCommand("$0", Seq("/bin/bash"))

    assert(cmd.size === 1)
    assert(cmd.head === "/bin/bash")
  }

  test("processing a command that is multiple words should split the string") {
    val cmd = GenomicRDD.processCommand("tee /dev/null", Seq.empty)

    assert(cmd.size === 2)
    assert(cmd(0) === "tee")
    assert(cmd(1) === "/dev/null")
  }

  test("process a command that is multiple words with a replacement") {
    val cmd = GenomicRDD.processCommand("echo $0", Seq("/path/to/my/file"))

    assert(cmd.size === 2)
    assert(cmd(0) === "echo")
    assert(cmd(1) === "/path/to/my/file")
  }

  test("process a command that is multiple words with multiple replacements") {
    val cmd = GenomicRDD.processCommand("aCommand $0 hello $1", Seq("/path/to/my/file",
      "/path/to/another/file"))

    assert(cmd.size === 4)
    assert(cmd(0) === "aCommand")
    assert(cmd(1) === "/path/to/my/file")
    assert(cmd(2) === "hello")
    assert(cmd(3) === "/path/to/another/file")
  }
}
