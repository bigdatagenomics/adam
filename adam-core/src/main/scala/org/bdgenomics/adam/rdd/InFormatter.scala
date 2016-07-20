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
package org.bdgenomics.adam.rdd

import java.io.OutputStream

private[rdd] class InFormatterRunner[T, U <: GenomicRDD[T, U], V <: InFormatter[T, U, V]](iter: Iterator[T],
                                                                                          formatter: V,
                                                                                          os: OutputStream) extends Runnable {

  def run() {
    formatter.write(os, iter)
    os.flush()
    os.close()
  }
}

trait InFormatterCompanion[T, U <: GenomicRDD[T, U], V <: InFormatter[T, U, V]] {

  def apply(gRdd: U): V
}

/**
 * Formats data going into a pipe to an invoked process.
 *
 * @tparam T The type of records being formatted.
 */
trait InFormatter[T, U <: GenomicRDD[T, U], V <: InFormatter[T, U, V]] extends Serializable {

  protected val companion: InFormatterCompanion[T, U, V]

  /**
   * Writes records from an iterator into an output stream.
   *
   * @param os An OutputStream connected to a process we are piping to.
   * @param iter An iterator of records to write.
   */
  def write(os: OutputStream, iter: Iterator[T])
}

