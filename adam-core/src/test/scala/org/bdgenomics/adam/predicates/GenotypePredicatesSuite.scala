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
package org.bdgenomics.adam.predicates

import java.io.File
import java.util.logging.Level

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ ParquetLogger, SparkFunSuite }
import org.bdgenomics.formats.avro.{ Contig, Genotype, Variant, VariantCallingAnnotations }

class GenotypePredicatesSuite extends SparkFunSuite {

  sparkTest("Load only only PASSing records") {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val v0 = Variant.newBuilder
      .setContig(Contig.newBuilder.setContigName("chr11").build)
      .setStart(17409571)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val passFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(true).build()
    val failFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(false).build()

    val genotypes = sc.parallelize(List(
      Genotype.newBuilder()
        .setVariant(v0)
        .setVariantCallingAnnotations(passFilterAnnotation)
        .setSampleId("NA12878")
        .build(),
      Genotype.newBuilder()
        .setVariant(v0)
        .setVariantCallingAnnotations(failFilterAnnotation)
        .setSampleId("NA12878")
        .build()))

    val genotypesParquetFile = new File(Files.createTempDir(), "genotypes")
    genotypes.adamSave(genotypesParquetFile.getAbsolutePath)

    val gts1: RDD[Genotype] = sc.adamLoad(
      genotypesParquetFile.getAbsolutePath,
      predicate = Some(classOf[GenotypeRecordPASSPredicate]))
    assert(gts1.count === 1)

    FileUtils.deleteDirectory(genotypesParquetFile.getParentFile)
  }

  sparkTest("Load all records and filter to only PASSing records") {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val v0 = Variant.newBuilder
      .setContig(Contig.newBuilder.setContigName("11").build)
      .setStart(17409571)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val passFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(true).build()
    val failFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(false).build()

    val genotypes = sc.parallelize(List(
      Genotype.newBuilder().setVariant(v0)
        .setSampleId("ignored")
        .setVariantCallingAnnotations(passFilterAnnotation).build(),
      Genotype.newBuilder()
        .setSampleId("ignored")
        .setVariant(v0)
        .setVariantCallingAnnotations(failFilterAnnotation).build()))

    val genotypesParquetFile = new File(Files.createTempDir(), "genotypes")
    genotypes.adamSave(genotypesParquetFile.getAbsolutePath)

    val gts: RDD[Genotype] = sc.adamLoad(genotypesParquetFile.getAbsolutePath)
    assert(gts.count === 2)

    val predicate = new GenotypeRecordPASSPredicate
    val filtered = predicate(gts)
    assert(filtered.count === 1)

    FileUtils.deleteDirectory(genotypesParquetFile.getParentFile)
  }
}
