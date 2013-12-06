/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.commands

import org.scalatest._
import edu.berkeley.cs.amplab.adam.avro.VariantType

class Vcf2AdamSuite extends FunSuite {

  test("VcfConverter.getType() is able to return all indicates VariantType values") {

    import VcfConverter._

    assert( getType("A", "T") === VariantType.SNP )
    assert( getType("AA", "AT") === VariantType.MNP )
    assert( getType("A", "AA") === VariantType.Insertion )
    assert( getType("AA", "A") === VariantType.Deletion )
    assert( getType("AAA", "AA,ATGC") === VariantType.Complex )
    assert( getType("A", "<AA") === VariantType.SV )
  }
}
