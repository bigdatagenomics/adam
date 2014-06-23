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
package org.bdgenomics.adam.io

import java.io._

/**
 * ByteAccess is a wrapper trait around sources of bytes which are accessible at arbitrary offsets.
 *
 * (We can use this to wrap byte buffers, S3, FileInputStreams, or even range-accessible web services.)
 */
trait ByteAccess {
  def length(): Long
  def readByteStream(offset: Long, length: Int): InputStream

  /**
   * readFully is a helper method, for when you're going to use readByteStream and just read
   * all the bytes immediately.
   * @param offset The offset in the resource at which you want to start reading
   * @param length The number of bytes to read
   * @return An Array of bytes read. The length of this array will be <= the 'length' argument.
   */
  def readFully(offset: Long, length: Int): Array[Byte] = {
    assert(length >= 0, "length %d should be non-negative".format(length))
    assert(offset >= 0, "offset %d should be non-negative".format(offset))
    var totalBytesRead: Int = 0
    val buffer = new Array[Byte](length)
    val is = readByteStream(offset, length)
    while (totalBytesRead < length) {
      val bytesRead = is.read(buffer, totalBytesRead, length - totalBytesRead)
      totalBytesRead += bytesRead
    }
    buffer
  }
}
