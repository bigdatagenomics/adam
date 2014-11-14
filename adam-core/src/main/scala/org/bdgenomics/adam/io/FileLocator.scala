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

  /**
   * Creates a FileLocator that represents the 'parent' (if one exists) of the current
   * Locator.  For example, if the current locator is a file, then this should return
   * a locator for the directory in which the file lives.  If the current locator is an
   * S3 key, then the parent locator is determined by dividing the key on the '/' character
   * and returning the key with the last /-delimited segment removed, and so on.
   *
   * @return Some(locator) if the parent exists, None otherwise.
   */
  def parentLocator(): Option[FileLocator]

  /**
   * Searches and returns an iterable of FileLocators representing the children (if they
   * exist) of the current Locator.  For example, if the current locator is an S3FileLocator,
   * then this does a "ListObjects" query against the S3 interface and returns the objects
   * which are children to this key.  If the current Locator is a directory in a filesystem,
   * then the childLocators returns the list of Locators for files/directories within this
   * directory, and so on.
   *
   * @return An iterable of FileLocators, each corresponding to a child of this locator; the
   *         iterable is empty if none exist.
   */
  def childLocators(): Iterable[FileLocator]

  /**
   * Returns a 'relative' Locator, whose location is relative to the current locator.
   * This is equivalent (in spirit) to the operation of 'relative' URLs and resolution of
   * URLs relative to a base URL, in the HTTP URL spec (and indeed, it defaults to that
   * resolution in the case of HTTPFileLocator, which wraps URLs directly).
   *
   * In other types of FileLocators, this creates relative paths in a filesystem (for
   * LocalFileLocator) or relative keys (by appending) in S3FileLocator.
   *
   * @param relativePath The relative fragment of a location, given as a string. The returned
   *                     FileLocator will have a location that is the location of this base
   *                     Locator 'plus' the relative string, combined in a system-appropriate way.
   * @return The relative FileLocator created by resolving the 'relativePath' against the
   *         location of this base Locator.
   */
  def relativeLocator(relativePath: String): FileLocator

  /**
   * Gives direct access to the bytes of the resource underlying this locator.
   *
   * @return A ByteAccess object allowing random access to the bytes of the underlying resource.
   */
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
