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

import org.bdgenomics.utils.instrumentation.Metrics

/**
 * Contains [[Timers]] that are used to instrument ADAM.
 */
object Timers extends Metrics {

  // Load methods
  val LoadAlignments = timer("Load Alignments")
  val LoadContigFragments = timer("Load Contig Fragments")
  val LoadCoverage = timer("Load Coverage")
  val LoadFeatures = timer("Load Features")
  val LoadFragments = timer("Load Fragments")
  val LoadGenotypes = timer("Load Genotypes")
  val LoadReferenceFile = timer("Load ReferenceFile")
  val LoadSequenceDictionary = timer("Load SequenceDictionary")
  val LoadVariants = timer("Load Variants")

  // Format specific load methods
  val LoadBam = timer("Load BAM/CRAM/SAM format")
  val LoadBed = timer("Load BED6/12 format")
  val LoadFasta = timer("Load FASTA format")
  val LoadFastq = timer("Load FASTQ format")
  val LoadGff3 = timer("Load GFF3 format")
  val LoadGtf = timer("Load GTF/GFF2 format")
  val LoadIndexedBam = timer("Load indexed BAM format")
  val LoadIndexedVcf = timer("Load indexed VCF format")
  val LoadInterleavedFastq = timer("Load interleaved FASTQ format")
  val LoadInterleavedFastqFragments = timer("Load interleaved FASTQ format as Fragments")
  val LoadIntervalList = timer("Load IntervalList format")
  val LoadNarrowPeak = timer("Load NarrowPeak format")
  val LoadPairedFastq = timer("Load paired FASTQ format")
  val LoadPairedFastqFragments = timer("Load paired FASTQ format as Fragments")
  val LoadParquet = timer("Load Parquet + Avro format")
  val LoadUnpairedFastq = timer("Load unpaired FASTQ format")
  val LoadVcf = timer("Load VCF format")

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
  val CreatingKnownSnpsTable = timer("Creating Known SNPs Table")
  val CollectingSnps = timer("Collecting SNPs")
  val BroadcastingKnownSnps = timer("Broadcasting known SNPs")
  val ComputeCovariates = timer("Compute Covariates")
  val ObservingRead = timer("Observing covariates per read")
  val ReadCovariates = timer("Computing covariates per read base")
  val ComputingDinucCovariate = timer("Computing dinuc covariate")
  val ComputingCycleCovariate = timer("Computing cycle covariate")
  val ReadResidues = timer("Splitting read into residues")
  val CheckingForMask = timer("Checking if residue is masked")
  val ObservationAccumulatorComb = timer("Observation Accumulator: comb")
  val ObservationAccumulatorSeq = timer("Observation Accumulator: seq")
  val RecalibrateRead = timer("Recalibrate Read")
  val ComputeQualityScore = timer("Compute Quality Score")
  val GetExtraValues = timer("Get Extra Values")
  val CreatingRecalibrationTable = timer("Creating recalibration table")
  val InvertingRecalibrationTable = timer("Inverting recalibration table")
  val QueryingRecalibrationTable = timer("Querying recalibration table")

  // Realign Indels
  val RealignIndelsInDriver = timer("Realign Indels")
  val FindTargets = timer("Find Targets")
  val CreateIndelRealignmentTargets = timer("Create Indel Realignment Targets for Read")
  val SortTargets = timer("Sort Targets")
  val JoinTargets = timer("Join Targets")
  val MapTargets = timer("Map Targets")
  val RealignTargetGroup = timer("Realign Target Group")
  val GetReferenceFromReads = timer("Get Reference From Reads")
  val GetReferenceFromFile = timer("Get Reference From File")
  val ComputingOriginalScores = timer("Computing Original Mismatch Scores")
  val SweepReadsOverConsensus = timer("Sweeping Reads Over A Single Consensus")
  val SweepReadOverReferenceForQuality = timer("Sweep Read Over Reference For Quality")
  val FinalizingRealignments = timer("Finalizing Realignments")

  // Sort Reads
  val SortReads = timer("Sort Reads")
  val SortByIndex = timer("Sort Reads By Index")

  // File Saving
  val SAMSave = timer("SAM Save")
  val ConvertToSAM = timer("Convert To SAM")
  val ConvertToSAMRecord = timer("Convert To SAM Record")
  val SaveAsADAM = timer("Save File In ADAM Format")
  val SaveAsVcf = timer("Save File In VCF Format")
  val WriteADAMRecord = timer("Write ADAM Record")
  val WriteBAMRecord = timer("Write BAM Record")
  val WriteSAMRecord = timer("Write SAM Record")
  val WriteCRAMRecord = timer("Write CRAM Record")

  // org.bdgenomics.adam.rdd.TreeRegionJoin
  val TreeJoin = timer("Running broadcast join with interval tree")
  val BuildingTrees = timer("Building interval tree")
  val SortingRightSide = timer("Sorting right side of join")
  val GrowingTrees = timer("Growing forest of trees")
  val RunningMapSideJoin = timer("Running map-side join")

  // org.bdgenomics.adam.rdd.GenomicDataset
  val InnerBroadcastJoin = timer("Inner broadcast region join")
  val RightOuterBroadcastJoin = timer("Right outer broadcast region join")
  val BroadcastJoinAndGroupByRight = timer("Broadcast join followed by group-by on right")
  val RightOuterBroadcastJoinAndGroupByRight = timer("Right outer broadcast join followed by group-by on right")
  val InnerShuffleJoin = timer("Inner shuffle region join")
  val RightOuterShuffleJoin = timer("Right outer shuffle region join")
  val LeftOuterShuffleJoin = timer("Left outer shuffle region join")
  val FullOuterShuffleJoin = timer("Full outer shuffle region join")
  val ShuffleJoinAndGroupByLeft = timer("Shuffle join followed by group-by on left")
  val RightOuterShuffleJoinAndGroupByLeft = timer("Right outer shuffle join followed by group-by on left")
}
