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
import org.apache.avro.generic.GenericRecord
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.utils.cli.Args4j
import org.bdgenomics.utils.misc.HadoopUtil

class FlattenSuite extends ADAMFunSuite {

  sparkTest("can flatten a simple VCF file") {

    val loader = Thread.currentThread().getContextClassLoader
    val inputPath = loader.getResource("small.vcf").getPath
    val outputFile = File.createTempFile("adam-cli.FlattenSuite", ".adam")
    val outputPath = outputFile.getAbsolutePath
    val flatFile = File.createTempFile("adam-cli.FlattenSuite", ".adam-flat")
    val flatPath = flatFile.getAbsolutePath

    assert(outputFile.delete(), "Couldn't delete (empty) temp file")
    assert(flatFile.delete(), "Couldn't delete (empty) temp file")

    val argLine = "%s %s".format(inputPath, outputPath).split("\\s+")
    val args: Vcf2ADAMArgs = Args4j.apply[Vcf2ADAMArgs](argLine)
    val vcf2Adam = new Vcf2ADAM(args)
    vcf2Adam.run(sc)

    val lister = new ParquetLister[Genotype]()
    val records = lister.materialize(outputPath).toSeq

    assert(records.size === 15)
    assert(records(0).getSampleId === "NA12878")
    assert(records(0).getVariant.getStart == 14396L)
    assert(records(0).getVariant.getEnd == 14400L)

    val flattenArgLine = "%s %s".format(outputPath, flatPath).split("\\s+")
    val flattenArgs: FlattenArgs = Args4j.apply[FlattenArgs](flattenArgLine)
    val flatten = new Flatten(flattenArgs)
    flatten.run(sc)

    val flatLister = new ParquetLister[GenericRecord]()
    val flatRecords = flatLister.materialize(flatPath).toSeq

    assert(flatRecords.size === 15)
    assert(flatRecords(0).get("sampleId") === "NA12878")
    assert(flatRecords(0).get("variant__start") === 14396L)
    assert(flatRecords(0).get("variant__end") === 14400L)
  }

}
