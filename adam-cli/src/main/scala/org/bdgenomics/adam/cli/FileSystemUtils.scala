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
package org.bdgenomics.adam.cli

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileAlreadyExistsException

/**
 * Utility methods for file systems.
 */
private[cli] object FileSystemUtils {
  private def exists(pathName: String, conf: Configuration): Boolean = {
    val p = new Path(pathName)
    val fs = p.getFileSystem(conf)
    fs.exists(p)
  }

  // move to BDGSparkCommand in bdg-utils?
  def checkWriteablePath(pathName: String, conf: Configuration): Unit = {
    if (exists(pathName, conf)) {
      throw new FileAlreadyExistsException("Cannot write to path name, %s already exists".format(pathName))
    }
  }
}
