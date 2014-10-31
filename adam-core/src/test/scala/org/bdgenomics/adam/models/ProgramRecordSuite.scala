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
package org.bdgenomics.adam.models

import htsjdk.samtools.SAMProgramRecord
import org.scalatest.FunSuite

class ProgramRecordSuite extends FunSuite {

  test("convert a sam program record with no optional fields into a program record and v/v") {
    val spr = new SAMProgramRecord("myProgramRecord")
    val pr = ProgramRecord(spr)

    assert(pr.id === "myProgramRecord")
    assert(pr.commandLine.isEmpty)
    assert(pr.name.isEmpty)
    assert(pr.version.isEmpty)
    assert(pr.previousID.isEmpty)

    val convert = pr.toSAMProgramRecord()
    assert(spr.equals(convert))
  }

  test("convert a sam program record into a program record and v/v") {
    val spr = new SAMProgramRecord("myProgramRecord")
    spr.setCommandLine("command")
    spr.setProgramName("myCommand")
    spr.setProgramVersion("0.0.0")
    spr.setPreviousProgramGroupId("myPreviousProgramRecord")

    val pr = ProgramRecord(spr)

    assert(pr.id === "myProgramRecord")
    assert(pr.commandLine.get === "command")
    assert(pr.name.get === "myCommand")
    assert(pr.version.get === "0.0.0")
    assert(pr.previousID.get === "myPreviousProgramRecord")

    val convert = pr.toSAMProgramRecord()
    assert(spr.equals(convert))
  }
}
