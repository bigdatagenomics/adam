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

import java.io.InputStream
import java.util.concurrent.Callable

private[rdd] class OutFormatterRunner[T, U <: OutFormatter[T]](formatter: U,
                                                               is: InputStream) extends Callable[Iterator[T]] {

  def call(): Iterator[T] = {
    formatter.read(is)
  }
}

/**
 * Deserializes data coming out of a pipe from an invoked process.
 *
 * @tparam T The type of records being formatted.
 */
trait OutFormatter[T] extends Serializable {

  /**
   * Reads an iterator of records from an input stream.
   *
   * @param is The input stream coming from a process to read records from.
   * @return Returns an iterator of records that have been read from this
   *   stream.
   */
  def read(is: InputStream): Iterator[T]
}

