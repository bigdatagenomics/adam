/*
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
package org.bdgenomics.adam.models

import java.io.{ FileInputStream, File }
import java.util.zip.GZIPInputStream

import org.apache.spark.SparkContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import GeneFeatureRDD._
import GeneContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.adam.rdd.features.ADAMFeaturesContext._

import scala.io.Source

class GeneSuite extends SparkFunSuite {

  ignore("can load a set of gene models from an Ensembl GTF file") {
    val path = testFile("features/Homo_sapiens.GRCh37.75.chr20.gtf")
    val features: RDD[GTFFeature] = sc.adamGTFFeatureLoad(path)

    val genes: RDD[Gene] = features.asGenes()
    assert(genes.count() === 1317)

    val transcripts = genes.flatMap(_.transcripts)
    assert(transcripts.count() === 4040)

    val exons = transcripts.flatMap(_.exons)
    assert(exons.count() === 25376)
  }

  ignore("can load a set of gene models from a Gencode GTF file") {
    val path = testFile("features/gencode.v19.annotation.chr20.gtf")
    val features: RDD[GTFFeature] = sc.adamGTFFeatureLoad(path)

    val genes: RDD[Gene] = features.asGenes()
    assert(genes.count() === 1317)

    val transcripts = genes.flatMap(_.transcripts)
    assert(transcripts.count() === 4040)

    val exons = transcripts.flatMap(_.exons)
    assert(exons.count() === 25376)
  }

  def loadGzippedFa(path: String): Map[String, String] = {
    val buildable = scala.collection.mutable.Map[String, StringBuilder]()
    var current: String = null

    val fis = new GZIPInputStream(new FileInputStream(new File(path)))
    Source.fromInputStream(fis).getLines().foreach { line =>
      if (line.startsWith(">")) {
        current = line.substring(1, line.length())
        buildable.put(current, new StringBuilder())
      } else {
        buildable.get(current).foreach(_.append(line.trim))
      }
    }

    buildable.map {
      case (key: String, builder: StringBuilder) => (key, builder.toString())
    }.toMap
  }

  sparkTest("chr20 gencode transcript sequences match the published sequences") {
    val path = testFile("features/gencode.v19.annotation.chr20.gtf")
    val chr20path = testFile("chr20.fa.gz")
    val gencodeTranscriptsFa = testFile("gencode.v19.pc_transcripts.fa.gz")

    val chr20sequence =
      loadGzippedFa(chr20path).values.headOption

    val gencodeTranscripts = loadGzippedFa(gencodeTranscriptsFa).map {
      case (key: String, seq: String) =>
        (key.split("\\|").head, seq)
    }

    val features: RDD[GTFFeature] = sc.adamGTFFeatureLoad(path)
    val genes: RDD[Gene] = features.asGenes()
    val transcripts: Seq[Transcript] = genes.flatMap(g => g.transcripts).take(100)

    transcripts.foreach { transcript =>
      val mySequence = transcript.extractSplicedmRNASequence(chr20sequence.get)
      val gencodeSequence = gencodeTranscripts.get(transcript.id)

      if (gencodeSequence.isDefined) {
        assert(gencodeSequence.isDefined, "Transcript %s doesn't have a gencode seq".format(transcript.id))
        val gseq: String = gencodeSequence.get

        val exonLengths = (if (transcript.strand)
          transcript.exons.toSeq.sortBy(_.region.start)
        else
          transcript.exons.toSeq.sortBy(_.region.start).reverse).map(_.region.length())

        val exonStarts = exonLengths.scanLeft(0)((sum, v) => sum + v.toInt)
        val gseqExons: Array[String] = exonStarts.zip(exonLengths).map {
          case (start, len) => gseq.substring(start, start + len.toInt)
        }.toArray
        val myExons: Array[String] = exonStarts.zip(exonLengths).map {
          case (start, len) => mySequence.substring(start, start + len.toInt)
        }.toArray

        assert(gseqExons.length === myExons.length)
        (0 until myExons.length).foreach { i =>
          assert(myExons(i) === gseqExons(i), "Exons %d don't match".format(i))
        }

        assert(mySequence === gseq,
          "Transcript %s (%s:%d-%d:%s) didn't match: \n%s\ndidn't match gencode\n%s\n".format(
            transcript.id, transcript.region.referenceName,
            transcript.region.start, transcript.region.end, transcript.strand,
            mySequence, gencodeSequence.getOrElse("NO SEQUENCE")))
      }
    }
  }

}
