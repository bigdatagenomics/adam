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

import java.io._
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import scala.Serializable

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

class ByteArrayByteAccess(val bytes: Array[Byte]) extends ByteAccess with Serializable {

  private val inputStream = new ByteArrayInputStream(bytes)
  assert(inputStream.markSupported(), "ByteArrayInputStream doesn't support marks")

  inputStream.mark(bytes.length)

  override def length(): Long = bytes.length
  override def readByteStream(offset: Long, length: Int): InputStream = {
    inputStream.reset()
    inputStream.skip(offset)
    inputStream
  }
}

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

class S3ByteAccess(client: AmazonS3, bucket: String, keyName: String) extends ByteAccess {
  assert(bucket != null)
  assert(keyName != null)

  lazy val objectMetadata = client.getObjectMetadata(bucket, keyName)
  override def length(): Long = objectMetadata.getContentLength
  override def readByteStream(offset: Long, length: Int): InputStream = {
    val getObjectRequest = new GetObjectRequest(bucket, keyName).withRange(offset, offset + length)
    client.getObject(getObjectRequest).getObjectContent
  }

}
