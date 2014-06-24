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

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import java.io.File

/**
 * FileLocator is a trait which is meant to combine aspects of
 * - Java's File
 * - Hadoop's Path
 * - S3 locations (including bucket and key)
 * - classpath-relative URLs (classpath://, used in testing)
 *
 * It provides methods for relative addressing (parent and child locators,
 * which are equivalent to the File(parent, child) constructor and the getParentFile method
 * on the Java File class), as well as accessing the bytes named by a locator
 * by retrieving a ByteAccess value.
 *
 * We're using implementations of FileLocator to provide a uniform access interface
 * to Parquet files, whether they're in HDFS, a local filesystem, S3, or embedded in the
 * classpath as part of tests.
 */
trait FileLocator extends Serializable {

  def parentLocator(): Option[FileLocator]
  def relativeLocator(relativePath: String): FileLocator
  def bytes: ByteAccess
}

object FileLocator {

  val slashDivided = "^(.*)/([^/]+/?)$".r

  def parseSlash(path: String): Option[(String, String)] =
    slashDivided.findFirstMatchIn(path) match {
      case None    => None
      case Some(m) => Some(m.group(1), m.group(2))
    }
}

class S3FileLocator(val credentials: AWSCredentials, val bucket: String, val key: String) extends FileLocator {

  override def parentLocator(): Option[FileLocator] = FileLocator.parseSlash(key) match {
    case Some((parent, child)) => Some(new S3FileLocator(credentials, bucket, parent))
    case None                  => None
  }

  override def relativeLocator(relativePath: String): FileLocator =
    new S3FileLocator(credentials, bucket, "%s/%s".format(key.stripSuffix("/"), relativePath))

  override def bytes: ByteAccess = new S3ByteAccess(new AmazonS3Client(credentials), bucket, key)
}

class LocalFileLocator(val file: File) extends FileLocator {
  override def relativeLocator(relativePath: String): FileLocator = new LocalFileLocator(new File(file, relativePath))
  override def bytes: ByteAccess = new LocalFileByteAccess(file)

  override def parentLocator(): Option[FileLocator] = file.getParentFile match {
    case null             => None
    case parentFile: File => Some(new LocalFileLocator(parentFile))
  }

  override def hashCode(): Int = file.hashCode()
  override def equals(x: Any): Boolean = {
    x match {
      case loc: LocalFileLocator => file.equals(loc.file)
      case _                     => false
    }
  }
}

class ByteArrayLocator(val byteData: Array[Byte]) extends FileLocator {
  override def relativeLocator(relativePath: String): FileLocator = this
  override def parentLocator(): Option[FileLocator] = None
  override def bytes: ByteAccess = new ByteArrayByteAccess(byteData)
}
