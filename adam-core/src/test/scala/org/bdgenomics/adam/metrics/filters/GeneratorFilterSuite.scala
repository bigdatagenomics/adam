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
package org.bdgenomics.adam.metrics.filters

import org.bdgenomics.adam.metrics
import org.scalatest._

class GeneratorFilterSuite extends FunSuite {

  test("CombinedFilter combines two independent filters") {
    val f1 = new ComparisonsFilter[Int](null) {
      def passesFilter(value: Any): Boolean = value.asInstanceOf[Int] > 5
    }
    val f2 = new ComparisonsFilter[Int](null) {
      def passesFilter(value: Any): Boolean = value.asInstanceOf[Int] > 10
    }

    val pass1 = 10
    val fail1 = 5
    val pass2 = 20

    assert(f1.passesFilter(pass1))
    assert(!f1.passesFilter(fail1))
    assert(f2.passesFilter(pass2))

    val pass = metrics.Collection(Seq(Seq(pass1), Seq(pass2)))
    val fail = metrics.Collection(Seq(Seq(fail1), Seq(pass2)))

    val f12 = new CombinedFilter[Int](Seq(f1, f2))

    assert(f12.passesFilter(pass))
    assert(!f12.passesFilter(fail))
  }
}
