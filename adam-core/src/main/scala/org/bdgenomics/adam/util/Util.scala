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

import org.bdgenomics.formats.avro.Contig

object Util {
  def isSameContig(left: Contig, right: Contig): Boolean = {
    val leftName = Option(left).map(_.getContigName)
    val leftMD5 = Option(left).map(_.getContigMD5)
    val rightName = Option(right).map(_.getContigName)
    val rightMD5 = Option(right).map(_.getContigMD5)
    leftName == rightName && (leftMD5.isEmpty || rightMD5.isEmpty || leftMD5 == rightMD5)
  }
}
