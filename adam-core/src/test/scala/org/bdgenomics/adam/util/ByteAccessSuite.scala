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
package org.bdgenomics.adam.util

import org.scalatest.FunSuite
import java.io.{ PrintWriter, File, ByteArrayInputStream }
import java.nio.charset.Charset

class ByteAccessSuite extends FunSuite {

  test("ByteArrayByteAccess returns arbitrary subsets of bytes correctly") {
    val bytes = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val access = new ByteArrayByteAccess(bytes)

    assert(access.length() === bytes.length)
    assert(access.readFully(5, 5) === bytes.slice(5, 10))
  }

  test("ByteArrayByteAccess supports two successive calls with different offsets") {
    val bytes = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val access = new ByteArrayByteAccess(bytes)

    assert(access.length() === bytes.length)
    assert(access.readFully(5, 5) === bytes.slice(5, 10))
    assert(access.readFully(3, 5) === bytes.slice(3, 8))
  }

  test("LocalFileByteAccess returns arbitrary subsets of bytes correctly") {
    val content = "abcdefghij"
    val temp = File.createTempFile("byteaccesssuite", "test")

    val writer = new PrintWriter(temp)
    writer.print(content)
    writer.close()

    val access = new LocalFileByteAccess(temp)
    assert(access.length() === content.length())
    assert(access.readFully(3, 5) === content.substring(3, 8).getBytes("ASCII"))
  }
}
