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

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantDataset
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Variant

class SnpTableSuite extends ADAMFunSuite {

  test("create an empty snp table") {
    val table = SnpTable()
    assert(table.indices.isEmpty)
    assert(table.sites.isEmpty)
  }

  sparkTest("create a snp table from variants on multiple contigs") {
    val inputPath = testFile("random.vcf")
    val table = SnpTable(sc.loadVariants(inputPath))
    assert(table.indices.size === 3)
    assert(table.indices("1") === (0, 2))
    assert(table.indices("2") === (3, 3))
    assert(table.indices("13") === (4, 5))
    assert(table.sites.length === 6)
    assert(table.sites(0) === 14396L)
    assert(table.sites(1) === 14521L)
    assert(table.sites(2) === 63734L)
    assert(table.sites(3) === 19189L)
    assert(table.sites(4) === 752720L)
    assert(table.sites(5) === 752790L)
  }

  sparkTest("create a snp table from a larger set of variants") {
    val inputPath = testFile("bqsr1.vcf")
    val variants = sc.loadVariants(inputPath)
    val numVariants = variants.rdd.count
    val table = SnpTable(variants)
    assert(table.indices.size === 1)
    assert(table.indices("22") === (0, numVariants - 1))
    assert(table.sites.length === numVariants)
    val variantsByPos = variants.rdd
      .map(v => v.getStart.toInt)
      .collect
      .sorted
    table.sites
      .zip(variantsByPos)
      .foreach(p => {
        assert(p._1 === p._2)
      })
  }

  def lookUpVariants(rdd: VariantDataset): SnpTable = {
    val table = SnpTable(rdd)
    val variants = rdd.rdd.collect

    variants.foreach(v => {
      val sites = table.maskedSites(ReferenceRegion(v))
      assert(sites.size === 1)
    })

    table
  }

  sparkTest("perform lookups on multi-contig snp table") {
    val inputPath = testFile("random.vcf")
    val variants = sc.loadVariants(inputPath)
    val table = lookUpVariants(variants)

    val s1 = table.maskedSites(ReferenceRegion("1", 14390L, 14530L))
    assert(s1.size === 2)
    assert(s1(14396L))
    assert(s1(14521L))

    val s2 = table.maskedSites(ReferenceRegion("13", 752700L, 752800L))
    assert(s2.size === 2)
    assert(s2(752720L))
    assert(s2(752790L))
  }

  sparkTest("perform lookups on larger snp table") {
    val inputPath = testFile("bqsr1.vcf")
    val variants = sc.loadVariants(inputPath)
    val table = lookUpVariants(variants)

    val s1 = table.maskedSites(ReferenceRegion("22", 16050670L, 16050690L))
    assert(s1.size === 2)
    assert(s1(16050677L))
    assert(s1(16050682L))

    val s2 = table.maskedSites(ReferenceRegion("22", 16050960L, 16050999L))
    assert(s2.size === 3)
    assert(s2(16050966L))
    assert(s2(16050983L))
    assert(s2(16050993L))

    val s3 = table.maskedSites(ReferenceRegion("22", 16052230L, 16052280L))
    assert(s3.size === 4)
    assert(s3(16052238L))
    assert(s3(16052239L))
    assert(s3(16052249L))
    assert(s3(16052270L))
  }
}
