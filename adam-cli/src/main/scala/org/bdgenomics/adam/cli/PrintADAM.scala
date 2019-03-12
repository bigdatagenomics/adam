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

import java.util
import org.apache.avro.generic.{ GenericDatumWriter, IndexedRecord }
import org.apache.avro.io.EncoderFactory
import org.apache.spark.SparkContext
import org.bdgenomics.adam.cli.FileSystemUtils._
import org.bdgenomics.adam.util.ParquetFileTraversable
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import scala.collection.JavaConversions._

object PrintADAM extends BDGCommandCompanion {
  val commandName: String = "print"
  val commandDescription: String = "Print an ADAM formatted file"

  def apply(cmdLine: Array[String]) = {
    new PrintADAM(Args4j[PrintADAMArgs](cmdLine))
  }
}

class PrintADAMArgs extends Args4jBase {
  @Argument(required = true, metaVar = "FILE(S)", multiValued = true, usage = "One or more files to print")
  var filesToPrint = new util.ArrayList[String]()

  @Args4jOption(required = false, name = "-o", metaVar = "FILE", usage = "Output to a (local) file")
  var outputFile: String = _

  @Args4jOption(required = false, name = "-pretty", usage = "Display raw, pretty-formatted JSON")
  var prettyRaw: Boolean = false
}

class PrintADAM(protected val args: PrintADAMArgs) extends BDGSparkCommand[PrintADAMArgs] {
  val companion = PrintADAM

  /**
   * Safe output-stream “resource” function
   *
   * This function calls the function op on a java.io.PrintStream, if filename
   * is None, it uses System.out. If the function ends with an exception,
   * and filename is Some(_), the output stream will be closed.
   *
   * @param filename If provided, this is the output to print to. Else, if None,
   * we print to System.out.
   */
  def withPrintStream(filename: Option[String])(op: java.io.PrintStream => Unit) {
    val p = filename match {
      case Some(s) => new java.io.PrintStream(s)
      case None    => scala.Console.out
    }
    try { op(p) } finally { if (filename.isDefined) p.close() }
  }

  /**
   * Display any Parquet file using JSON
   */
  def displayRaw(sc: SparkContext, file: String, pretty: Boolean = false, output: Option[String] = None) {
    withPrintStream(output)(out => {
      val it = new ParquetFileTraversable[IndexedRecord](sc, file)
      pretty match {
        case true =>
          it.headOption match {
            case None => out.print("")
            case Some(hd) =>
              val schema = hd.getSchema
              val writer = new GenericDatumWriter[Object](schema)
              val encoder = EncoderFactory.get().jsonEncoder(schema, out)
              val jg = new org.codehaus.jackson.JsonFactory().createJsonGenerator(out)
              jg.useDefaultPrettyPrinter()
              encoder.configure(jg)
              it.foreach(pileup => {
                writer.write(pileup, encoder)
              })
          }
        case false =>
          it.foreach(pileup => {
            out.println(pileup.toString)
          })
      }
    })
  }

  def run(sc: SparkContext) {
    val output = Option(args.outputFile)
    output.foreach(checkWriteablePath(_, sc.hadoopConfiguration))
    args.filesToPrint.foreach(file => {
      displayRaw(sc, file, pretty = args.prettyRaw, output = output)
    })
  }
}
