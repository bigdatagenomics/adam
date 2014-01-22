/*
 * Copyright (c) 2014. Cloudera Inc.
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

package edu.berkeley.cs.amplab.adam.rdd.annotation

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.{Strand, ADAMAnnotation}
import edu.berkeley.cs.amplab.adam.rdd.annotation.ADAMAnnotationContext._
import scala.io.Source

class AnnotationParsingSuite extends SparkFunSuite {

  sparkTest("Can read a .bed file") {
    val metadata = new AnnotationMetadata("gencode", null, null)
    val path = ClassLoader.getSystemClassLoader.getResource("annotation/gencode.v7.annotation.trunc10.bed").getFile
    val annot: RDD[ADAMAnnotation] = sc.adamAnnotLoad(path, metadata)
    assert(annot.count === 10)
  }

  sparkTest("Can read a .narrowPeak file") {
    val metadata = new AnnotationMetadata("wgEncodeOpenChromDnase", null, "Gm19238")
    val path = ClassLoader.getSystemClassLoader.getResource("annotation/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak").getFile
    val annot: RDD[ADAMAnnotation] = sc.adamAnnotLoad(path, metadata)
    assert(annot.count === 10)
  }

  sparkTest("Can correctly parse a .bed file") {
    val path = ClassLoader.getSystemClassLoader.getResource("annotation/gencode.v7.annotation.trunc10.bed")
    val line = Source.fromURL(path).getLines().next()
    val parser = new BEDParser()
    val annot = parser.parse(line)
    assert(annot.getContig.getContigName === "chr1")
    assert(annot.getStart === 11869)
    assert(annot.getEnd === 14409)
    assert(annot.getId === "gene")
    assert(annot.getScore === null)
    assert(annot.getStrand === Strand.Plus)
    assert(annot.getLongVal === null)
    assert(annot.getStringVal === null)
    assert(annot.getDoubleVal === null)
    assert(annot.getPValue === null)
    assert(annot.getQValue === null)
    assert(annot.getPeak === null)
    assert(annot.getName === null)
    assert(annot.getExperimentId === null)
    assert(annot.getSampleId === null)
  }

  sparkTest("Can correctly parse a .narrowPeak file") {
    val path = ClassLoader.getSystemClassLoader.getResource("annotation/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val line = Source.fromURL(path).getLines().next()
    val metadata = new AnnotationMetadata("wgEncodeOpenChromDnase", null, "Gm19238")
    val parser = new NarrowPeakParser(metadata)
    val annot = parser.parse(line)
    assert(annot.getContig.getContigName === "chr1")
    assert(annot.getStart === 713849)
    assert(annot.getEnd === 714434)
    assert(annot.getId === "chr1.1")
    assert(annot.getScore === 1000)
    assert(annot.getStrand === null)
    assert(annot.getLongVal === null)
    assert(annot.getStringVal === null)
    assert(annot.getDoubleVal === "0.2252".toDouble)
    assert(annot.getPValue === "9.16".toDouble)
    assert(annot.getQValue === null)
    assert(annot.getPeak === 263)
    assert(annot.getName === "wgEncodeOpenChromDnase")
    assert(annot.getExperimentId === null)
    assert(annot.getSampleId === "Gm19238")
  }

}
