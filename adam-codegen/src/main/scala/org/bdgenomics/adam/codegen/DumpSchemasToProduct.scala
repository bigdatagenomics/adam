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
import scala.collection.JavaConversions._

object DumpSchemasToProduct {

  def main(args: Array[String]) {
    new DumpSchemasToProduct()(args)
  }
}

class DumpSchemasToProduct extends Generator {

  private def toMatch(fields: Seq[(String, String)]): String = {
    fields.map(_._1)
      .zipWithIndex
      .map(vk => {
        val (field, idx) = vk
        "    case %d => %s".format(idx, field)
      }).mkString("\n")
  }

  private def getType(schema: Schema): String = schema.getType match {
    case Schema.Type.DOUBLE  => "Double"
    case Schema.Type.FLOAT   => "Float"
    case Schema.Type.INT     => "Int"
    case Schema.Type.LONG    => "Long"
    case Schema.Type.BOOLEAN => "Boolean"
    case Schema.Type.STRING  => "String"
    case Schema.Type.ENUM    => "String"
    case Schema.Type.RECORD  => schema.getName()
    case other               => throw new IllegalStateException("Unsupported type %s.".format(other))
  }

  private def getUnionType(schema: Schema): Schema = {
    val unionTypes = schema.getTypes()
      .filter(t => {
        t.getType != Schema.Type.NULL
      })
    assert(unionTypes.size == 1)
    unionTypes.head
  }

  private def fields(schema: Schema): Seq[(String, String)] = {
    schema.getFields()
      .map(field => {
        val name = field.name
        val fieldSchema = field.schema
        val fieldType = fieldSchema.getType match {
          case Schema.Type.ARRAY => {
            "Seq[%s] = Seq()".format(getType(fieldSchema.getElementType()))
          }
          case Schema.Type.MAP => {
            "scala.collection.Map[String,%s] = Map()".format(getType(fieldSchema.getValueType()))
          }
          case Schema.Type.UNION => {
            "Option[%s] = None".format(getType(getUnionType(fieldSchema)))
          }
          case other => {
            throw new IllegalStateException("Unsupported type %s in field %s.".format(other, name))
          }
        }
        (name, fieldType)
      })
  }

  private def conversion(schema: Schema, mapFn: String): String = schema.getType match {
    case Schema.Type.DOUBLE  => ".%s(d => d: java.lang.Double)".format(mapFn)
    case Schema.Type.FLOAT   => ".%s(f => f: java.lang.Float)".format(mapFn)
    case Schema.Type.INT     => ".%s(i => i: java.lang.Integer)".format(mapFn)
    case Schema.Type.LONG    => ".%s(l => l: java.lang.Long)".format(mapFn)
    case Schema.Type.BOOLEAN => ".%s(b => b: java.lang.Boolean)".format(mapFn)
    case Schema.Type.STRING  => ""
    case Schema.Type.ENUM    => ".%s(e => %s.valueOf(e))".format(mapFn, schema.getFullName)
    case Schema.Type.RECORD  => ".%s(r => r.toAvro)".format(mapFn)
    case other               => throw new IllegalStateException("Unsupported type %s.".format(other))
  }

  private def setters(schema: Schema): String = {
    schema.getFields
      .map(field => {
        val name = field.name

        field.schema.getType match {
          case Schema.Type.UNION => {
            getUnionType(field.schema).getType match {
              case Schema.Type.RECORD => "    %s.foreach(field => record.set%s(field.toAvro))".format(name, name.capitalize)
              case Schema.Type.ENUM   => "    %s.foreach(field => record.set%s(%s.valueOf(field)))".format(name, name.capitalize, getUnionType(field.schema).getFullName)
              case Schema.Type.DOUBLE | Schema.Type.FLOAT |
                Schema.Type.INT | Schema.Type.LONG |
                Schema.Type.BOOLEAN | Schema.Type.STRING => "    %s.foreach(field => record.set%s(field))".format(name, name.capitalize)
              case other => throw new IllegalStateException("Unsupported type %s.".format(other))
            }
          }
          case Schema.Type.ARRAY => {
            val convAction = conversion(field.schema.getElementType(), "map")
            "    if (%s.nonEmpty) {\n      record.set%s(%s%s)\n    } else {\n      record.set%s(new java.util.LinkedList())\n    }".format(name, name.capitalize, name, convAction, name.capitalize)
          }
          case Schema.Type.MAP => {
            val convAction = conversion(field.schema.getValueType(), "mapValues")
            "    if (%s.nonEmpty) {\n      record.set%s(%s%s.asJava)\n    } else {\n      record.set%s(new java.util.HashMap())\n    }".format(name, name.capitalize, name, convAction, name.capitalize)
          }
          case _ => {
            throw new IllegalArgumentException("Bad type %s.".format(field.schema))
          }
        }
      }).mkString("\n")
  }

  private def dumpToAvroFn(schema: Schema): String = {
    "    val record = new %s()\n%s\n    record".format(schema.getFullName,
      setters(schema))
  }

  private def generateClassDump(className: String): String = {

    // get schema
    val schema = ReflectSchema.getSchemaByReflection(className)

    // get class name without package
    val classNameNoPackage = className.split('.').last

    "\n%s\n\ncase class %s (\n%s) extends Product {\n  def productArity: Int = %d\n  def productElement(i: Int): Any = i match {\n%s\n  }\n  def toAvro: %s = {\n%s\n  }\n  def canEqual(that: Any): Boolean = that match {\n    case %s => true\n    case _ => false\n  }\n}".format(
      dumpObject(schema),
      classNameNoPackage,
      fields(schema).map(p => "  %s: %s".format(p._1, p._2)).mkString(",\n"),
      schema.getFields().size,
      toMatch(fields(schema)),
      schema.getFullName,
      dumpToAvroFn(schema),
      classNameNoPackage
    )
  }

  private def getConversion(schema: Schema, mapFn: String): String = schema.getType match {
    case Schema.Type.DOUBLE  => ".%s(d => d: Double)".format(mapFn)
    case Schema.Type.FLOAT   => ".%s(f => f: Float)".format(mapFn)
    case Schema.Type.INT     => ".%s(i => i: Int)".format(mapFn)
    case Schema.Type.LONG    => ".%s(l => l: Long)".format(mapFn)
    case Schema.Type.BOOLEAN => ".%s(b => b: Boolean)".format(mapFn)
    case Schema.Type.STRING  => ""
    case Schema.Type.ENUM    => ".%s(e => e.toString)".format(mapFn)
    case Schema.Type.RECORD  => ".%s(r => %s.fromAvro(r))".format(mapFn, schema.getName)
    case other               => throw new IllegalStateException("Unsupported type %s.".format(other))
  }

  private def getters(schema: Schema): String = {
    schema.getFields
      .map(field => {
        val name = field.name

        field.schema.getType match {
          case Schema.Type.UNION => {
            getUnionType(field.schema).getType match {
              case Schema.Type.RECORD => "      Option(record.get%s).map(field => %s.fromAvro(field))".format(name.capitalize, getUnionType(field.schema).getName)
              case Schema.Type.ENUM   => "      Option(record.get%s).map(field => field.toString)".format(name.capitalize)
              case Schema.Type.DOUBLE | Schema.Type.FLOAT |
                Schema.Type.INT | Schema.Type.LONG |
                Schema.Type.BOOLEAN | Schema.Type.STRING => "    Option(record.get%s)%s".format(name.capitalize, getConversion(getUnionType(field.schema), "map"))
              case other => throw new IllegalStateException("Unsupported type %s.".format(other))
            }
          }
          case Schema.Type.ARRAY => {
            val convAction = getConversion(field.schema.getElementType(), "map")
            "      record.get%s().toSeq%s".format(name.capitalize, convAction)
          }
          case Schema.Type.MAP => {
            val convAction = getConversion(field.schema.getValueType(), "mapValues")
            "      record.get%s()%s.asScala".format(name.capitalize, convAction)
          }
          case _ => {
            throw new IllegalArgumentException("Bad type %s.".format(field.schema))
          }
        }
      }).mkString(",\n")
  }

  private def dumpObject(schema: Schema): String = {
    "object %s extends Serializable {\n  def fromAvro(record: %s): %s = {\n    new %s (\n%s)\n  }\n}".format(
      schema.getName,
      schema.getFullName,
      schema.getName,
      schema.getName,
      getters(schema))
  }

  def apply(args: Array[String]) {

    if (args.length < 3) {
      println("DumpSchemas <package-name> <class-to-dump> ... <file-to-dump-to>")
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
