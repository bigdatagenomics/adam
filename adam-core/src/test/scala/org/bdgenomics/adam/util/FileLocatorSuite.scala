/**
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.util

import org.scalatest.FunSuite
import java.io.{ PrintWriter, FileWriter, File }

class FileLocatorSuite extends FunSuite {

  test("parseSlash can correctly parse a one-slash string") {
    assert(FileLocator.parseSlash("foo/bar") === Some(("foo", "bar")))
    assert(FileLocator.parseSlash("/foo") === Some(("", "foo")))
  }

  test("parseSlash can correctly parse a two-slash string") {
    assert(FileLocator.parseSlash("foo/bar") === Some(("foo", "bar")))
    assert(FileLocator.parseSlash("/foo/bar") === Some(("/foo", "bar")))
  }

  test("parseSlash can correctly parse a no-slash string") {
    assert(FileLocator.parseSlash("foo") === None)
  }
}

class LocalFileLocatorSuite extends FunSuite {

  test("parentLocator retrieves the parent directory") {
    val temp: File = File.createTempFile("LocalFileLocatorSuite", "test")
    val loc: FileLocator = new LocalFileLocator(temp)
    val parentLocOpt: Option[FileLocator] = loc.parentLocator()

    parentLocOpt match {
      case Some(parentLoc) => assert(parentLoc === new LocalFileLocator(temp.getParentFile))
      case None            => fail("parentLoc wasn't defined")
    }
  }

  test("relativeLocator retrieves a subdirectory") {
    val temp1: File = File.createTempFile("LocalFileLocatorSuite", "test")
    val tempDir = temp1.getParentFile

    val tempDirLoc = new LocalFileLocator(tempDir)
    val tempLoc = tempDirLoc.relativeLocator(temp1.getName)

    assert(tempLoc === new LocalFileLocator(temp1))
  }

  test("bytes accesses the named underlying file") {
    val temp = File.createTempFile("LocalFileLocatorSuite", "test")
    val pw = new PrintWriter(new FileWriter(temp))
    pw.println("abcdefghij")
    pw.close()

    val loc = new LocalFileLocator(temp)
    val io = loc.bytes

    val str = new String(io.readFully(3, 3), "UTF-8")
    assert(str === "def")
  }
}

class ByteArrayLocatorSuite extends FunSuite {
  test("byte access can be wrapped in a locator correctly") {
    val bytes = Array[Byte](1, 2, 3, 4, 5)
    val loc = new ByteArrayLocator(bytes)

    val io = loc.bytes
    val read = io.readFully(2, 3)
    assert(read === Array[Byte](3, 4, 5))
  }
}
