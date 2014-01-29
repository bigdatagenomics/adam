/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.converters

import scala.collection.JavaConverters._
import org.scalatest.FunSuite
import org.broadinstitute.variant.variantcontext.{Allele, VariantContextBuilder}

class VariantContextConverterSuite extends FunSuite {
  test("Convert site-only SNV") {
    val vc = new VariantContextBuilder()
      .alleles(List(Allele.create("A",true), Allele.create("T")).asJavaCollection)
      .start(1L)
      .stop(1L)
      .chr("1")
      .make()

    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)

    val adamVC = adamVCs.head
    assert(adamVC.genotypes.length === 0)

    val variant = adamVC.variant
    assert(variant.getReferenceAllele === "A")
    assert(variant.getPosition === 0L)
  }
}
