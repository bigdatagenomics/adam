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

import java.io._

import org.bdgenomics.adam.parquet_reimpl.ParquetLister
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.projections.ADAMFeatureField._
import org.bdgenomics.formats.avro.ADAMFeature
import org.scalatest.FunSuite

class Features2ADAMSuite extends FunSuite {

  ignore("can convert a simple BED file") {

    val loader = Thread.currentThread().getContextClassLoader
    val inputPath = loader.getResource("features/gencode.v7.annotation.trunc10.bed").getPath
    val outputFile = File.createTempFile("adam-cli.Features2ADAMSuite", ".adam")
    val outputPath = outputFile.getAbsolutePath

    val argLine = "%s %s".format(inputPath, outputPath).split("\\s+")

    // We have to do this, since the features2adam won't work if the file already exists,
    // but the "createTempFile" method actually creates the file (on some systems?)
    assert(outputFile.delete(), "Couldn't delete (empty) temp file")

    val args: Features2ADAMArgs = Args4j.apply[Features2ADAMArgs](argLine)

    val features2Adam = new Features2ADAM(args)
    features2Adam.run()

    val schema = Projection(featureId, contig, start, strand)
    val lister = new ParquetLister[ADAMFeature](Some(schema))

    val converted = lister.materialize(outputPath).toSeq

    assert(converted.size === 10)

    /*
    val types = converted.groupBy(_.getFeatureType).map {
      case (key: String, value: Seq[ADAMFeature]) => (key, value.length)
    }

    assert(types.contains("exon"))
    assert(types.get("exon").get === 7)
    assert(types.get("transcript").get === 2)
    assert(types.get("gene").get === 1)
    */

    assert(converted.find(_.getContig.getContigName != "chr1").isEmpty)
  }

}
