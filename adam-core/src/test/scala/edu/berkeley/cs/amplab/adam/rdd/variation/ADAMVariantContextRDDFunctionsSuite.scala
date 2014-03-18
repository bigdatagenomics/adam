/*
 * Copyright (c) 2013. Mount Sinai School of Medicine
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
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._

class ADAMVariantContextRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("joins SNV database annotation") {
    val v0 = ADAMVariant.newBuilder
      .setContig(ADAMContig.newBuilder.setContigName("11").setContigId(11).build)
      .setPosition(17409572)
      .setReferenceAllele("T")
      .setVariantAllele("C")
      .build

    val vc: RDD[ADAMVariantContext] = sc.parallelize(List(
      ADAMVariantContext(v0)
    ))

    val a0 = ADAMDatabaseVariantAnnotation.newBuilder
      .setVariant(v0)
      .setDbSnpId(5219)
      .build

    val vda: RDD[ADAMDatabaseVariantAnnotation] = sc.parallelize(List(
      a0
    ))

    // TODO: implicit conversion to ADAMVariantContextRDD
    val annotated = vc.joinDatabaseVariantAnnotation(vda)
    assert( annotated.map(_.databases.isDefined).reduce { (a,b) => a && b } )
  }

}
