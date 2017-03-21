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
import java.lang.Process
import java.util.concurrent.Callable

private[rdd] class OutFormatterRunner[T, U <: OutFormatter[T]](formatter: U,
                                                               is: InputStream,
                                                               process: Process,
                                                               finalCmd: List[String]) extends Iterator[T] {

  private val iter = formatter.read(is)

  def hasNext: Boolean = {
    if (iter.hasNext) {
      true
    } else {
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new RuntimeException("Piped command %s exited with error code %d.".format(
          finalCmd, exitCode))
      }

      false
    }
  }

  def next: T = {
    assert(iter.hasNext)
    iter.next
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

