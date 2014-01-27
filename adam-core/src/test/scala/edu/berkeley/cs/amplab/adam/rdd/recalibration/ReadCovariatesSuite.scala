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

package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SnpTable

class ReadCovariatesSuite extends SparkFunSuite {

  test("Test Quality Offset"){

    val read = ADAMRecord.newBuilder()
      .setRecordGroupId(0)
      .setReadMapped(true).setStart(10000)
      .setReferenceName("1")
      .setCigar("10M")
      .setMismatchingPositions("5C4")
      .setSequence("CTACCCTAAC")
      .setQual("##LKLPPQ##")
      .build()
    var readCovar = ReadCovariates( read, new QualByRG(), List(new BaseContext(2)), SnpTable() )
    val firstBaseCovar = readCovar.next()
    assert(firstBaseCovar.qual === 43 )
    readCovar.foreach(bc => assert(bc.qual === bc.qualByRG) )

    readCovar = ReadCovariates( read, new QualByRG(), List(new BaseContext(2)), SnpTable() )
    val bases = readCovar.drop(3)
    val mismatchedBase = bases.next()
    assert(mismatchedBase.qual === 47)
    assert(mismatchedBase.isMismatch === true)

  }

  test("Test ReadCovar on SoftClipped Read"){

    val read = ADAMRecord.newBuilder()
      .setRecordGroupId(0)
      .setReadMapped(true).setStart(10000)
      .setReferenceName("1")
      .setCigar("2S6M2S")
      .setMismatchingPositions("3C2")
      .setSequence("CTACCCTAAC")
      .setQual("##LKLPPQ##")
      .build()
    var readCovar = ReadCovariates( read, new QualByRG(), List(new BaseContext(2)), SnpTable() )
    val firstBaseCovar = readCovar.next()
    assert(firstBaseCovar.qual === 43 )
    readCovar.foreach(bc => assert(bc.qual === bc.qualByRG) )

    readCovar = ReadCovariates( read, new QualByRG(), List(new BaseContext(2)), SnpTable() )
    val bases = readCovar.drop(3)
    val mismatchedBase = bases.next()
    assert(mismatchedBase.qual === 47)
    assert(mismatchedBase.isMismatch === true)

  }

}
