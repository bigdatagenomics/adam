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

  // Note: Prior to factoring out Contig from AlignmentRecord, isSameContig below
  // did a more comprehensive comparison of contig identity by
  // comparing the ContigMD5 of left and right.  We may want to revisit this in the future
  // and once again include this MD5 check for completeness.
  //
  // See the code as of commit hash b8e36b2 for this previous version which included
  // the md5 comparison

  def isSameContig(left: String, right: String): Boolean = {
    left == right
  }

}
