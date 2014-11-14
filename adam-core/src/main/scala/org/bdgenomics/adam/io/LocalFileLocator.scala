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

  override def childLocators(): Iterable[FileLocator] = {
    val filter = new FileFilter {
      override def accept(p1: File): Boolean = p1.isFile
    }
    file.listFiles(filter).map { f =>
      new LocalFileLocator(f)
    }
  }
}
