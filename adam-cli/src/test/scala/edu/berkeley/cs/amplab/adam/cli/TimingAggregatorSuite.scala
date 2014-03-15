/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.scalatest.BeforeAndAfterEach

class TimingAggregatorSuite extends FunSuite with BeforeAndAfterEach {

  // Set up the temp file needed by the test
  override def beforeEach() {
    TimingAggregator.Reset
  }

  test("Basic start/stop test") {
    TimingAggregator.Start("foo")
    TimingAggregator.Stop("foo", "exception happened")
    val tempFile = File.createTempFile("xxx", null).getPath()
    TimingAggregator.Flush(tempFile)
    val lines = scala.io.Source.fromFile(tempFile).getLines.toList

    assert(lines.size == 1)

    val entry = lines(0).split(",")

    // Should have 4 components because there's a note
    assert(entry.size == 4)
    // Assert phase name and note
    assert(entry(0) == "foo")
    assert(entry(3) == "exception happened")
  }

  test("Basic start/stop test with no note") {
    TimingAggregator.Start("foo2")
    TimingAggregator.Stop("foo2")
    val tempFile = File.createTempFile("xxx", null).getPath()
    TimingAggregator.Flush(tempFile)
    val lines = scala.io.Source.fromFile(tempFile).getLines.toList

    assert(lines.size == 1)

    val entry = lines(0).split(",")
    // Should have 3 components because there's no note.
    assert(entry.size == 3)
    // Test phase name
    assert(entry(0) == "foo2")
  }

  test("Output file is appended to, not overwritten, and multiple flush calls do not rewrite the same entries") {
    TimingAggregator.Start("appendtest")
    TimingAggregator.Stop("appendtest")
    val tempFile = File.createTempFile("xxx", null).getPath()
    TimingAggregator.Flush(tempFile)
    TimingAggregator.Start("appendtest2")
    TimingAggregator.Stop("appendtest2")
    TimingAggregator.Flush(tempFile)

    val lines = scala.io.Source.fromFile(tempFile).getLines.toList

    // Two entries
    assert(lines.size == 2)

    val firstLine = lines(0).split(",")

    // Should have 3 components because there no note
    assert(firstLine.size == 3)
    // Assert phase name and note
    assert(firstLine(0) == "appendtest")

    val secondLine = lines(1).split(",")

    // Should have 3 components because there no note
    assert(secondLine.size == 3)
    // Assert phase name and note
    assert(secondLine(0) == "appendtest2")

  }
}
