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

import org.bdgenomics.formats.avro.ADAMContig

object Util {
  def isSameContig(left: ADAMContig, right: ADAMContig): Boolean = {
    left.getContigName == right.getContigName && (
      left.getContigMD5 == null || right.getContigMD5 == null || left.getContigMD5 == right.getContigMD5)
  }

  def hashCombine(parts: Int*): Int =
    if (parts.tail == Nil)
      parts.head
    else
      hashCombine2(parts.head, hashCombine(parts.tail: _*))

  // Based on hash_combine from the C++ Boost library
  private def hashCombine2(first: Int, second: Int) =
    second + 0x9E3779B9 + (first << 6) + (first >> 2)
}
