/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.commands

import org.apache.hadoop.fs.{FileSystem, Path}
import parquet.avro.AvroParquetReader
import org.apache.avro.generic.IndexedRecord
import scala.collection.JavaConversions._
import edu.berkeley.cs.amplab.adam.util.{Args4jBase, Args4j}
import org.kohsuke.args4j.Argument
import java.util

object PrintAdam {
  def main(args: Array[String]) {
    new PrintAdam().commandExec(args)
  }
}

class PrintAdamArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "FILE(S)", multiValued = true, usage = "One or more files to print")
  var filesToPrint = new util.ArrayList[String]()
}

class PrintAdam extends AdamCommand with SparkCommand {
  val commandName: String = "print"
  val commandDescription: String = "Print an ADAM formatted file"

  def commandExec(cmdArgs: Array[String]) {
    val args = Args4j[PrintAdamArgs](cmdArgs)
    val sc = createSparkContext(args)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    args.filesToPrint.foreach {
      fileToPrint =>
        val inputPath = new Path(fileToPrint)
        if (!fs.exists(inputPath)) {
          throw new IllegalArgumentException("The path %s does not exist".format(fileToPrint))
        }
        var paths = List[Path]()
        if (fs.isDirectory(inputPath)) {
          val files = fs.listStatus(inputPath)
          files.foreach {
            file =>
              if (file.getPath.getName.startsWith("part-")) {
                paths ::= file.getPath
              }
          }
        }
        else if (fs.isFile(inputPath)) {
          paths ::= inputPath
        } else {
          throw new IllegalArgumentException("The path '%s' is neither file nor directory".format(fileToPrint))
        }

        paths.foreach {
          path =>
            val parquetReader = new AvroParquetReader(path)
            var record: IndexedRecord = null
            do {
              record = parquetReader.read()
              if (record != null) {
                println(record)
              }
            } while (record != null)
            parquetReader.close()
        }
    }
  }
}
