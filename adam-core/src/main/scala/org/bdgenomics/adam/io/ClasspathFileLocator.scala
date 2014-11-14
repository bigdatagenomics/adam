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

import java.io.{ FileFilter, File }

class ClasspathFileLocator(classpath: String) extends FileLocator {
  override def relativeLocator(relativePath: String): FileLocator =
    new ClasspathFileLocator("%s/%s".format(classpath.stripSuffix("/"), relativePath))

  override def bytes: ByteAccess = {
    val url = Thread.currentThread().getContextClassLoader.getResource(classpath)
    if (url == null) { throw new IllegalArgumentException("Illegal classpath \"%s\"".format(classpath)) }
    val path = url.getFile
    val file = new File(path)
    println("Returning bytes from %s".format(file.getAbsolutePath))
    new LocalFileByteAccess(file)
  }

  override def parentLocator(): Option[FileLocator] = FileLocator.parseSlash(classpath) match {
    case Some((parent, child)) => Some(new ClasspathFileLocator(parent))
    case None                  => None
  }

  override def childLocators(): Iterable[FileLocator] = {
    val url = Thread.currentThread().getContextClassLoader.getResource(classpath)
    val f = new File(url.getFile)
    f.listFiles(new FileFilter() {
      override def accept(p1: File): Boolean = p1.isFile && p1.canRead
    }).map { file =>
      new LocalFileLocator(file)
    }
  }
}
