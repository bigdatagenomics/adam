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
import org.bdgenomics.adam.rdd.ADAMContext._

object ProgramRecord {

  def apply(pr: SAMProgramRecord): ProgramRecord = {
    // ID is a required field
    val id: String = pr.getId

    // these fields are optional and can be left null, so must check for null...
    val commandLine: Option[String] = Option(pr.getCommandLine)
    val name: Option[String] = Option(pr.getProgramName)
    val version: Option[String] = Option(pr.getProgramVersion)
    val previousID: Option[String] = Option(pr.getPreviousProgramGroupId)

    new ProgramRecord(id, commandLine, name, version, previousID)
  }
}

case class ProgramRecord(
    id: String,
    commandLine: Option[String],
    name: Option[String],
    version: Option[String],
    previousID: Option[String]) {

  def toSAMProgramRecord(): SAMProgramRecord = {
    val pr = new SAMProgramRecord(id)

    // set optional fields
    commandLine.foreach(cl => pr.setCommandLine(cl))
    name.foreach(n => pr.setProgramName(n))
    version.foreach(v => pr.setProgramVersion(v))
    previousID.foreach(id => pr.setPreviousProgramGroupId(id))

    pr
  }
}
