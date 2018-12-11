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
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.cli.Args4j

class TransformFeaturesSuite extends ADAMFunSuite {

  sparkTest("can convert a simple BED file") {

    val loader = Thread.currentThread().getContextClassLoader
    val inputPath = loader.getResource("gencode.v7.annotation.trunc10.bed").getPath
    val outputFile = File.createTempFile("adam-cli.TransformFeaturesSuite", ".adam")
    val outputPath = outputFile.getAbsolutePath

    val argLine = "%s %s".format(inputPath, outputPath).split("\\s+")

    // We have to do this, since the features2adam won't work if the file already exists,
    // but the "createTempFile" method actually creates the file (on some systems?)
    assert(outputFile.delete(), "Couldn't delete (empty) temp file")

    val args: TransformFeaturesArgs = Args4j.apply[TransformFeaturesArgs](argLine)

    val features2Adam = new TransformFeatures(args)
    features2Adam.run(sc)

    val converted = sc.loadFeatures(outputPath).rdd.collect

    assert(converted.size === 10)
    assert(converted.find(_.getReferenceName != "chr1").isEmpty)
  }
}
