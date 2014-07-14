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

import org.bdgenomics.adam.parquet_reimpl.ParquetLister
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.formats.avro.ADAMFlatGenotype
import org.kohsuke.args4j.Argument

import scala.collection.JavaConversions._
import org.bdgenomics.adam.projections.ADAMFlatGenotypeField._

object PrintParquet extends ADAMCommandCompanion {
  val commandName: String = "print_parquet"
  val commandDescription: String = "Prints the contents of a parquet file"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new PrintParquet(Args4j[PrintParquetArgs](cmdLine))
  }
}

class PrintParquetArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "PARQUET_FILE", usage = "The parquet file to print", index = 0)
  var parquetFile: String = null

  @Argument(required = true, metaVar = "COUNT", usage = "The number of records to print", index = 1)
  var count: Int = 0
}

class PrintParquet(args: PrintParquetArgs) extends ADAMCommand {
  val companion = PrintParquet

  def run() = {

    val schema = Projection(referenceName, position, sampleId, referenceAllele, alleles)
    val lister: ParquetLister[ADAMFlatGenotype] = new ParquetLister[ADAMFlatGenotype](Some(schema))

    lister.materialize(args.parquetFile).take(args.count).foreach {
      case rec: ADAMFlatGenotype =>
        println("%s\t%d\t%s\t%s\t%s".format(
          rec.getReferenceName, rec.getPosition.toInt,
          rec.getSampleId,
          rec.getReferenceAllele, rec.getAlleles.mkString(",")))
    }
  }
}
