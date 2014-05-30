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
package org.bdgenomics.adam.parquet_reimpl

import org.scalatest.FunSuite

class TypePathSuite extends FunSuite {
  test("Two tail-less type pathes equal") {
    assert(new TypePath("head") === new TypePath("head"))
  }

  test("Two identically tailed type pathes equal") {
    val tail = new TypePath("tail")
    assert(new TypePath("head", tail) === new TypePath("head", tail))
  }

  test("Two type pathes equal with separately constructed tails") {
    assert(new TypePath("head", new TypePath("tail")) === new TypePath("head", new TypePath("tail")))
  }
}
