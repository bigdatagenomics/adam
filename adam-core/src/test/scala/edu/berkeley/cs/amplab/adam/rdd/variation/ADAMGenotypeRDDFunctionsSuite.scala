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

package edu.berkeley.cs.amplab.adam.rdd.variation

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro._
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


class ADAMGenotypeRDDFunctionsSuite extends SparkFunSuite {
  def v0 = ADAMVariant.newBuilder
    .setContig(ADAMContig.newBuilder.setContigName("11").build)
    .setPosition(17409572)
    .setReferenceAllele("T")
    .setVariantAllele("C")
    .build

  sparkTest("concordance of identical and non-identical genotypes") {
    val gb = ADAMGenotype.newBuilder().setVariant(v0)
      .setSampleId("NA12878")
      .setAlleles(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt))

    val g0 = gb.build
    val g1 = gb.build
    val tables0 = sc.parallelize(Seq(g0)).concordanceWith(sc.parallelize(Seq(g1))).collectAsMap
    assert(tables0.size === 1)
    val table0 = tables0.getOrElse("NA12878", ConcordanceTable())
    assert(table0.total === 1)
    assert(table0.get(ADAMGenotypeType.HET, ADAMGenotypeType.HET) === 1)

    val g2 = gb.setAlleles(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)).build
    val table1 = sc.parallelize(Seq(g0))
      .concordanceWith(sc.parallelize(Seq(g2))).collectAsMap()
      .getOrElse("NA12878", ConcordanceTable())
    assert(table1.total === 1)
    assert(table1.get(ADAMGenotypeType.HET, ADAMGenotypeType.HOM_REF) === 1)
  }

  sparkTest("concordance of identical VCFs should be 1.0") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.vcf").getFile

    val gts : RDD[ADAMGenotype] = sc.adamVCFLoad(path).flatMap(_.genotypes)
    assert(gts.filter(_.getSampleId == "NA12878").count === 5)

    val tables = gts.concordanceWith(gts).collectAsMap
    assert(tables.size === 3L)

    val table0 = tables.getOrElse("NA12878", ConcordanceTable())
    assert(table0.total === 5L)
    assert(table0.concordance === 1.0)
  }
}
