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
package org.bdgenomics.adam.instrumentation

/**
 * Contains [[Timers]] that are used to instrument ADAM.
 */
object Timers extends Metrics {

  // File Loading
  val LoadAlignmentRecords = timer("Load Alignment Records")
  val BAMLoad = timer("BAM File Load")
  val ParquetLoad = timer("Parquet File Load")

  // Trim Reads
  val TrimReadsInDriver = timer("Trim Reads")
  val TrimRead = timer("Trim Reads")
  val TrimCigar = timer("Trim Cigar")
  val TrimMDTag = timer("Trim MD Tag")

  // Trim Low Quality Read Groups
  val TrimLowQualityInDriver = timer("Trim Low Quality Read Groups")

  // Mark Duplicates
  val MarkDuplicatesInDriver = timer("Mark Duplicates")
  val CreateReferencePositionPair = timer("Create Reference Position Pair")
  val PerformDuplicateMarking = timer("Perform Duplicate Marking")
  val ScoreAndMarkReads = timer("Score and Mark Reads")
  val MarkReads = timer("Mark Reads")

  // Recalibrate Base Qualities
  val BQSRInDriver = timer("Base Quality Recalibration")
  val CreateKnownSnpsTable = timer("Create Known SNPs Table")
  val RecalibrateRead = timer("Recalibrate Read")

  // Realign Indels
  val RealignIndelsInDriver = timer("Realign Indels")
  val FindTargets = timer("Find Targets")
  val CreateIndelRealignmentTargets = timer("Create Indel Realignment Targets for Read")
  val SortTargets = timer("Sort Targets")
  val JoinTargets = timer("Join Targets")
  val MapTargets = timer("Map Targets")
  val RealignTargetGroup = timer("Realign Target Group")
  val GetReferenceFromReads = timer("Get Reference From Reads")
  val SweepReadOverReferenceForQuality = timer("Sweep Read Over Reference For Quality")

  // Sort Reads
  val SortReads = timer("Sort Reads")

  // File Saving
  val SAMSave = timer("SAM Save")
  val ConvertToSAM = timer("Convert To SAM")
  val ConvertToSAMRecord = timer("Convert To SAM Record")
  val SaveAsADAM = timer("Save File In ADAM Format")
  val WriteADAMRecord = timer("Write ADAM Record")
  val WriteBAMRecord = timer("Write BAM Record")
  val WriteSAMRecord = timer("Write SAM Record")

}
