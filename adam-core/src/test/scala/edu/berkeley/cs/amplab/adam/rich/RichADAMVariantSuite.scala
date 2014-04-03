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

package edu.berkeley.cs.amplab.adam.rich

import org.scalatest.FunSuite
import edu.berkeley.cs.amplab.adam.avro.{ ADAMVariant, ADAMContig }

class RichADAMVariantSuite extends FunSuite {
  test("Equality without reference MD5") {
    val c1 = ADAMContig.newBuilder()
      .setContigId(0)
      .setContigName("chr1")
      .setContigMD5("1b22b98cdeb4a9304cb5d48026a85128")
      .build
    val c2 = ADAMContig.newBuilder()
      .setContigId(0)
      .setContigName("chr1")
      .build()

    val v1 = ADAMVariant.newBuilder()
      .setContig(c1)
      .setPosition(1)
      .setReferenceAllele("A")
      .setVariantAllele("T")
      .build
    val v2 = ADAMVariant.newBuilder(v1)
      .setContig(c2)
      .build

    assert(v1 != v2)

    val rv1: RichADAMVariant = v1
    val rv2: RichADAMVariant = v2

    assert(rv1 == rv2)
    assert(rv1.hashCode == rv2.hashCode)
  }

  test("Equality with reference MD5") {
    val c1 = ADAMContig.newBuilder()
      .setContigId(0)
      .setContigName("chr1")
      .setContigMD5("1b22b98cdeb4a9304cb5d48026a85128")
      .build
    val c2 = ADAMContig.newBuilder()
      .setContigId(0)
      .setContigName("chr1")
      .setContigMD5("1b22b98cdeb4a9304cb5d48026a85127")
      .build

    val v1 = ADAMVariant.newBuilder()
      .setContig(c1)
      .setPosition(1)
      .setReferenceAllele("A")
      .setVariantAllele("T")
      .build
    val v2 = ADAMVariant.newBuilder(v1)
      .setContig(c2)
      .build

    assert(v1 != v2)

    val rv1: RichADAMVariant = v1
    val rv2: RichADAMVariant = v2

    assert(rv1 != rv2)
    assert(rv1.hashCode == rv2.hashCode)
  }
}
