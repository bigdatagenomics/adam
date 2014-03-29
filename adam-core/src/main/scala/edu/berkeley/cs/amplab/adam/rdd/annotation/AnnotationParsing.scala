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

import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, Strand, ADAMAnnotation}

class AnnotationMetadata(val name: String = null, val experiment: String = null, val sample: String = null)
  extends Serializable {}


class BEDParser(val metadata: AnnotationMetadata = new AnnotationMetadata()) extends Serializable {

  def parse(line: String): ADAMAnnotation = {
    val fields = line.split("\\t")
    val numFields = fields.length
    val contig = new ADAMContig()
    val annot = new ADAMAnnotation()
    if (numFields > 0) {
      contig.setContigName((fields(0)))
      annot.setContig(contig)
    }
    if (numFields > 1)
      annot.setStart(fields(1).toLong)
    if (numFields > 2)
      annot.setEnd(fields(2).toLong)
    if (numFields > 3)
      annot.setId(fields(3))
    if (numFields > 4)
      annot.setLongVal(fields(4) match {
        case "." => null
        case _ => fields(4).toLong
      })
    if (numFields > 5)
      annot.setStrand(fields(5) match {
        case "+" => Strand.Plus
	      case "-" => Strand.Minus
	      case _ => null
      })
    annot.setName(metadata.name)
    annot.setExperimentId(metadata.experiment)
    annot.setSampleId(metadata.sample)
    annot
  }
}


class BroadPeakParser(val metadata: AnnotationMetadata = new AnnotationMetadata()) extends Serializable {
  def parse(line: String): ADAMAnnotation = {
    val fields = line.split("\\t")
    val numFields = fields.length
    val contig = new ADAMContig()
    val annot = new ADAMAnnotation()
    if (numFields > 0) {
      contig.setContigName((fields(0)))
      annot.setContig(contig)
    }
    if (numFields > 1)
      annot.setStart(fields(1).toLong)
    if (numFields > 2)
      annot.setEnd(fields(2).toLong)
    if (numFields > 3)
      annot.setId(fields(3) match {
        case "." => null
        case "" => null
        case _ => fields(3)
      })
    if (numFields > 4)
      annot.setScore(fields(4).toLong)
    if (numFields > 5)
      annot.setStrand(fields(5) match {
        case "+" => Strand.Plus
        case "-" => Strand.Minus
        case _ => null
      })
    if (numFields > 6)
      annot.setDoubleVal(fields(6).toDouble)
    if (numFields > 7)
      annot.setPValue(fields(7).toDouble match {
        case -1. => null
        case _ => fields(7).toDouble
      })
    if (numFields > 8)
      annot.setQValue(fields(8).toDouble match {
        case -1. => null
        case _ => fields(8).toDouble
      })

    annot.setName(metadata.name)
    annot.setExperimentId(metadata.experiment)
    annot.setSampleId(metadata.sample)
    annot
  }
}


class NarrowPeakParser(val metadata: AnnotationMetadata = new AnnotationMetadata()) extends Serializable {
  val broadParser = new BroadPeakParser(metadata)

  def parse(line: String): ADAMAnnotation = {
    val annot = broadParser.parse(line)
    val fields = line.split("\\t")
    val numFields = fields.length
    if (numFields > 9)
      annot.setPeak(fields(9).toLong)
    annot
  }
}


class GFF2Parser(val metadata: AnnotationMetadata = new AnnotationMetadata()) extends Serializable {

  /* TODO(laserson): attributes not yet supported */
  def parse(line: String): ADAMAnnotation = {
    val fields = line.split("\\t")
    val annot = new ADAMAnnotation()
    val contig = new ADAMContig()
    contig.setContigName((fields(0)))
    annot.setContig(contig)
    annot.setStart(fields(3).toLong - 1)
    annot.setEnd(fields(4).toLong)
    annot.setName(fields(2))
    annot.setDoubleVal(fields(5) match {
      case "." => null
      case _ => fields(5).toDouble
    })
    annot.setStrand(fields(6) match {
      case "+" => Strand.Plus
      case "-" => Strand.Minus
      case _ => null
    })
    annot.setExperimentId(metadata.experiment)
    annot.setSampleId(metadata.sample)
    annot
  }
}