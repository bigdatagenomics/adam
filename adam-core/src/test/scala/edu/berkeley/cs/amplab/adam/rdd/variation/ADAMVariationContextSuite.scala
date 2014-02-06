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

import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import java.lang.Float

class ADAMVariationContextSuite extends SparkFunSuite {
  sparkTest("can read a small .vcf file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.vcf").getFile
    // TODO: Why doesn't implict work here?
    val vcs: RDD[ADAMVariantContext] = ADAMVariationContext.sparkContextToADAMVariationContext(sc).adamVCFLoad(path)
    assert(vcs.count === 5)

    val vc = vcs.first
    assert(vc.genotypes.length === 3)

    val gt = vc.genotypes.head
    assert(gt.getVariantCallingAnnotations != null)
    assert(gt.getVariantCallingAnnotations.getReadDepth === 69)
    // Recall we are testing parsing, so we assert that our value is
    // the same as should have been parsed
    assert(gt.getVariantCallingAnnotations.getClippingRankSum === 
      Float.valueOf("0.138"))
  }

}
