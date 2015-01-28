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

import java.net.URI

class HTTPFileLocator(uri: URI, retries: Int = 3) extends FileLocator {

  override def parentLocator(): Option[FileLocator] = {
    val currentPath = uri.getPath
    val components = currentPath.split("/")
    val parentPath = components.slice(0, components.length - 1).mkString("/")
    val parentURI = new URI(uri.getScheme, uri.getHost, parentPath, "")
    Some(new HTTPFileLocator(parentURI, retries))
  }

  override def relativeLocator(relativePath: String): FileLocator =
    new HTTPFileLocator(uri.resolve(relativePath), retries)

  override def bytes: ByteAccess = new HTTPRangedByteAccess(uri, retries)

  /**
   * Unsupported in HTTPFileLocator.
   *
   * (It's not clear, short of an expection for the format of an HTTP resource, how we should
   * "list the children" of that resource.  In other words: do we expect a web-page with links
   * to come back? Should all those links designate children? Only relative links? It's not
   * clear.)
   *
   * @throws UnupportedOperationException since the operation isn't supported.
   */
  override def childLocators(): Iterable[FileLocator] = throw new UnsupportedOperationException()
}
