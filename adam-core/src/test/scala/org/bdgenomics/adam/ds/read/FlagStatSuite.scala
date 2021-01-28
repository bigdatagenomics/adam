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
package org.bdgenomics.adam.ds.read

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{ AlignmentField, Projection }
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Alignment

class FlagStatSuite extends ADAMFunSuite {

  sparkTest("Standard FlagStat test") {

    val inputPath = testFile("NA12878.sam")
    val projection = Projection(
      AlignmentField.readMapped,
      AlignmentField.mateMapped,
      AlignmentField.readPaired,
      AlignmentField.referenceName,
      AlignmentField.mateReferenceName,
      AlignmentField.primaryAlignment,
      AlignmentField.duplicateRead,
      AlignmentField.readMapped,
      AlignmentField.mateMapped,
      AlignmentField.readInFragment,
      AlignmentField.properPair,
      AlignmentField.mappingQuality,
      AlignmentField.failedVendorQualityChecks)

    val adamFile: RDD[Alignment] = sc.loadAlignments(inputPath, optProjection = Some(projection)).rdd

    val (failedVendorQuality, passedVendorQuality) = FlagStat(adamFile)

    def percent(fraction: Long, total: Long) = if (total == 0) 0.0 else 100.00 * fraction.toFloat / total

    assert(passedVendorQuality.total == 529 && failedVendorQuality.total == 36,
      ": The values of total passed and vendor quality were not the expected values")

    assert(passedVendorQuality.duplicatesPrimary.total == 59 &&
      failedVendorQuality.duplicatesPrimary.total == 16,
      ": The values of passed and failed vendor quality primary duplicates (total) were not the expected values")

    assert(passedVendorQuality.duplicatesPrimary.bothMapped == 58 &&
      failedVendorQuality.duplicatesPrimary.bothMapped == 15,
      ": The values of passed and failed vendor quality primary duplicates (both mapped) were not the expected values")

    assert(passedVendorQuality.duplicatesPrimary.onlyReadMapped == 1 &&
      failedVendorQuality.duplicatesPrimary.onlyReadMapped == 1,
      ": The values of passed and failed vendor quality primary duplicates (only read mapped) were not the expected values")

    assert(passedVendorQuality.duplicatesPrimary.crossChromosome == 0 &&
      failedVendorQuality.duplicatesPrimary.crossChromosome == 0,
      ": The values of passed and failed vendor quality primary duplicates were not the expected values")

    assert(passedVendorQuality.duplicatesSecondary.total == 0 &&
      failedVendorQuality.duplicatesSecondary.total == 0,
      ": The values of passed and failed vendor quality secondary duplicates (total) were not the expected values")

    assert(passedVendorQuality.duplicatesSecondary.bothMapped == 0 &&
      failedVendorQuality.duplicatesSecondary.bothMapped == 0,
      ": The values of passed and failed vendor quality secondary duplicates (both mapped) were not the expected values")

    assert(passedVendorQuality.duplicatesSecondary.onlyReadMapped == 0 &&
      failedVendorQuality.duplicatesSecondary.onlyReadMapped == 0,
      ": The values of passed and failed vendor quality secondary duplicates (only read mapped) were not the expected values")

    assert(passedVendorQuality.duplicatesSecondary.crossChromosome == 0 &&
      failedVendorQuality.duplicatesSecondary.crossChromosome == 0,
      ": The values of passed and failed vendor quality (cross chromosome) were not the expected values")

    assert(passedVendorQuality.mapped == 529 && failedVendorQuality.mapped == 36,
      ": The values of passed and failed vendor quality (mapped) were not the expected values")

    assert(percent(passedVendorQuality.mapped, passedVendorQuality.total) == 100.00,
      ": The values of percent passed vendor quality (mapped/total) were not the expected values")

    assert(percent(failedVendorQuality.mapped, failedVendorQuality.total) == 100.00,
      ": The values of percent failed vendor quality (mapped/total) were not the expected values")

    assert(passedVendorQuality.pairedInSequencing == 529 && failedVendorQuality.pairedInSequencing == 36,
      ": The values of passed and failed vendor quality (paired sequencing) were not the expected values")

    assert(passedVendorQuality.read1 == 258 && failedVendorQuality.read1 == 13,
      ": The values of passed and failed vendor quality (read1) were not the expected values")

    assert(passedVendorQuality.read2 == 271 && failedVendorQuality.read2 == 23,
      ": The values of passed and failed vendor quality (read2) were not the expected values")

    assert(passedVendorQuality.properlyPaired == 524 && failedVendorQuality.properlyPaired == 32,
      ": The values of passed and failed vendor quality (properly paired) were not the expected values")

    assert("%.2f".format(percent(passedVendorQuality.properlyPaired, passedVendorQuality.total)).toDouble == 99.05,
      ": The values of percent passed vendor quality (properly paired) were not the expected values")

    assert("%.2f".format(percent(failedVendorQuality.properlyPaired, failedVendorQuality.total)).toDouble == 88.89,
      ": The values of percent passed vendor quality (properly paired) were not the expected values")

    assert(passedVendorQuality.withSelfAndMateMapped == 524 && failedVendorQuality.withSelfAndMateMapped == 32,
      ": The values of passed and failed vendor quality (itself & mate mapped) were not the expected values")

    assert(passedVendorQuality.singleton == 5 && failedVendorQuality.singleton == 4,
      ": The values of passed and failed vendor quality (singletons) were not the expected values")

    assert("%.2f".format(percent(passedVendorQuality.singleton, passedVendorQuality.total)).toDouble == .95,
      ": The values of percent passed vendor quality (singletons) were not the expected values")

    assert("%.2f".format(percent(failedVendorQuality.singleton, failedVendorQuality.total)).toDouble == 11.11,
      ": The values of percent failed vendor quality (singletons) were not the expected values")

    assert(passedVendorQuality.withMateMappedToDiffChromosome == 0 && failedVendorQuality.withMateMappedToDiffChromosome == 0,
      ": The values of passed and failed vendor quality (mate mapped to a different chromosome) were not the expected values")

    assert(passedVendorQuality.withMateMappedToDiffChromosomeMapQ5 == 0 && failedVendorQuality.withMateMappedToDiffChromosomeMapQ5 == 0,
      ": The values of passed and failed vendor quality (mate mapped to a different chromosome, mapQ>=5) were not the expected values")
  }
}
