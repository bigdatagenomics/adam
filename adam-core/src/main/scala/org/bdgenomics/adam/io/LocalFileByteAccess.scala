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

import java.io.{ File, FileInputStream, InputStream }

/**
 * This is somewhat poorly named, it probably should be LocalFileByteAccess
 *
 * @param f the file to read bytes from
 */
class LocalFileByteAccess(f: File) extends ByteAccess {

  assert(f.isFile, "\"%s\" isn't a file".format(f.getAbsolutePath))
  assert(f.exists(), "File \"%s\" doesn't exist".format(f.getAbsolutePath))
  assert(f.canRead, "File \"%s\" can't be read".format(f.getAbsolutePath))

  override def length(): Long = f.length()

  override def readByteStream(offset: Long, length: Int): InputStream = {
    val fileIo = new FileInputStream(f)
    fileIo.skip(offset)
    fileIo
  }

}
