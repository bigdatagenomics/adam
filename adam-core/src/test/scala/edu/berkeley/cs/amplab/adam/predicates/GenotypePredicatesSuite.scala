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

package edu.berkeley.cs.amplab.adam.predicates

import com.google.common.io.Files
import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, ADAMVariant, ADAMGenotype}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.{ParquetLogger, SparkFunSuite}
import java.util.logging.Level
import java.io.File
import org.scalatest.BeforeAndAfter
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.avro.Base

class GenotypePredicatesSuite extends SparkFunSuite with BeforeAndAfter {
  var genotypesParquetFile: File   = null

  sparkBefore("genotypepredicatessuite_before") {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val v0 = ADAMVariant.newBuilder
      .setContig(ADAMContig.newBuilder.setContigName("11").build)
      .setPosition(17409571)
      .setReferenceAlleles(List(Base.T).asJava)
      .setVariantAlleles(List(Base.C).asJava)
      .build

    val genotypes = sc.parallelize(List(
      ADAMGenotype.newBuilder().setVariant(v0).setVarIsFiltered(true).build(),
      ADAMGenotype.newBuilder().setVariant(v0).build()
    ))

    genotypesParquetFile = new File(Files.createTempDir(), "genotypes")
    if (genotypesParquetFile.exists())
      FileUtils.deleteDirectory(genotypesParquetFile.getParentFile)

    genotypes.adamSave(genotypesParquetFile.getAbsolutePath)
  }

  after {
    FileUtils.deleteDirectory(genotypesParquetFile.getParentFile)
  }

  sparkTest("Return only PASSing records") {
    val gts1: RDD[ADAMGenotype] = sc.adamLoad(
      genotypesParquetFile.getAbsolutePath,
      predicate = Some(classOf[GenotypeVarFilterPASSPredicate])
    )
    assert(gts1.count === 1)
  }
}
