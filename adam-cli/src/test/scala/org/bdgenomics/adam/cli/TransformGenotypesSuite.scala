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
package org.bdgenomics.adam.cli

import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class TransformGenotypesSuite extends ADAMFunSuite {

  sparkTest("save a file sorted by contig index") {
    val inputPath = copyResource("random.vcf")
    val intermediatePath = tmpFile("genotypes.adam")
    val actualPath = tmpFile("sorted.vcf")
    val expectedPath = testFile("sorted.vcf")

    TransformGenotypes(
      Array(inputPath, intermediatePath)
    ).run(sc)

    TransformGenotypes(
      Array(intermediatePath, actualPath, "-sort_on_save", "-single")
    ).run(sc)

    checkFiles(expectedPath, actualPath)
  }

  sparkTest("save a lexicographically sorted file") {
    val inputPath = copyResource("random.vcf")
    val intermediatePath = tmpFile("genotypes.lex.adam")
    val actualPath = tmpFile("sorted.lex.vcf")
    val expectedPath = testFile("sorted.lex.vcf")

    TransformGenotypes(
      Array(inputPath, intermediatePath)
    ).run(sc)

    TransformGenotypes(
      Array(intermediatePath, actualPath, "-sort_lexicographically_on_save", "-single")
    ).run(sc)

    checkFiles(expectedPath, actualPath)
  }

  sparkTest("transform VCF without nested annotations") {
    val inputPath = copyResource("random.vcf")
    val intermediatePath = tmpFile("genotypes.adam")

    TransformGenotypes(
      Array(inputPath, intermediatePath)
    ).run(sc)

    val genotypes = sc.loadGenotypes(intermediatePath)
    val genotype = genotypes.sort().rdd.first()
    assert(genotype.getVariant().getAnnotation() == null)
  }

  sparkTest("transform VCF with nested annotations") {
    val inputPath = copyResource("random.vcf")
    val intermediatePath = tmpFile("genotypes.nested-annotations.adam")

    TransformGenotypes(
      Array(inputPath, intermediatePath, "-nested_annotations")
    ).run(sc)

    val genotypes = sc.loadGenotypes(intermediatePath)
    val genotype = genotypes.sort().rdd.first()
    assert(genotype.getVariant().getAnnotation() != null)
    assert(genotype.getVariant().getAnnotation().get("alleleCount") === 2)
  }
}
