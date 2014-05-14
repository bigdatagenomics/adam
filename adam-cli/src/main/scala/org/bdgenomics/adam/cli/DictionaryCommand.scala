/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.{ FileOutputStream, File }
import org.apache.commons.io.IOUtils
import org.bdgenomics.adam.models.SequenceDictionary
import net.sf.samtools.SAMFileReader

trait DictionaryCommand {
  private def getDictionaryFile(name: String): Option[File] = {
    val stream = ClassLoader.getSystemClassLoader.getResourceAsStream("dictionaries/" + name)
    if (stream == null)
      return None
    val file = File.createTempFile(name, ".dict")
    file.deleteOnExit()
    IOUtils.copy(stream, new FileOutputStream(file))
    Some(file)
  }

  private def getDictionary(file: File) = Some(SequenceDictionary(SAMFileReader.getSequenceDictionary(file)))

  def loadSequenceDictionary(file: File): Option[SequenceDictionary] = {
    if (file != null) {
      if (file.exists)
        getDictionary(file)
      else getDictionaryFile(file.getName) match {
        case Some(file) => getDictionary(file)
        case _          => None
      }
    } else None
  }
}
