/*
 * Copyright (c) 2014. Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.models

import org.scalatest.FunSuite
import net.sf.samtools.{Cigar, TextCigarCodec}

class ConsensusSuite extends FunSuite {

  test ("test the insertion of a consensus insertion into a reference") {   
    val c = Consensus("TCGA", ReferenceRegion(0, 10L, 11L))
    
    val ref = "AAAAAAAAAA"
    
    val cs = c.insertIntoReference(ref, 5L, 15L)
    
    assert(cs === "AAAAATCGAAAAAA")
  }

  test ("test the insertion of a consensus deletion into a reference") {   
    val c = Consensus("", ReferenceRegion(0, 10L, 16L))
    
    val ref = "AAAAATTTTT"
    
    val cs = c.insertIntoReference(ref, 5L, 15L)
    
    assert(cs === "AAAAA")
  }

  test ("inserting empty consensus returns the reference") {
    val ref = "AAAAAAAAAAAAA"
    val c = new Consensus ("", ReferenceRegion(0, 0L, 1L))

    val co = c.insertIntoReference(ref, 0, ref.length)

    assert (ref === co)
  }
  
}
