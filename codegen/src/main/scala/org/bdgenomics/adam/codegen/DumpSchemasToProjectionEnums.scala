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
package org.bdgenomics.adam.codegen

import java.io.{ File, FileWriter }
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import scala.collection.JavaConversions._

object DumpSchemasToProjectionEnums {

  def main(args: Array[String]) {
    new DumpSchemasToProjectionEnums()(args)
  }
}

class DumpSchemasToProjectionEnums extends Generator {

  private def fields(schema: Schema): Seq[String] = {
    schema.getFields()
      .map(field => field.name)
      .toSeq
  }

  private def generateClassDump(className: String): String = {

    // get schema
    val schema = ReflectSchema.getSchemaByReflection(className)

    // get class name without package
    val classNameNoPackage = className.split('.').last

    "\n\nobject %sField extends FieldEnumeration(%s.SCHEMA$) {\n  val %s = SchemaValue\n}".format(
      classNameNoPackage,
      className,
      fields(schema).mkString(", "))
  }

  def apply(args: Array[String]) {

    if (args.length < 3) {
      println("DumpSchemasToProjectionEnums <package-name> <class-to-dump> ... <file-to-dump-to>")
      System.exit(1)
    } else {

      // drop the file to write and the package name
      val classesToDump = args.drop(1).dropRight(1)

      // open the file to write
      val dir = new File(args.last).getParentFile
      if (!dir.exists()) {
        dir.mkdirs()
      }
      val fw = new FileWriter(args.last)

      // write the header
      writeHeader(fw, args.head)

      // loop and dump the classes
      classesToDump.foreach(className => {
        val dumpString = generateClassDump(className)

        fw.write("\n")
        fw.write(dumpString)
      })

      // we are done, so close and flush
      fw.close()
    }
  }
}
