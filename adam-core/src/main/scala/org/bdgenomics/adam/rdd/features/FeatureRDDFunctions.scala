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
package org.bdgenomics.adam.rdd.features

import com.google.common.collect.ComparisonChain
import java.util.Comparator
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.formats.avro.{ Feature, Strand }
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._

trait FeatureOrdering[T <: Feature] extends Ordering[T] {
  def allowNull(s: java.lang.String): java.lang.Integer = {
    if (s == null) {
      return null
    }
    java.lang.Integer.parseInt(s)
  }

  def compare(x: Feature, y: Feature) = {
    val doubleNullsLast: Comparator[java.lang.Double] = com.google.common.collect.Ordering.natural().nullsLast()
    val intNullsLast: Comparator[java.lang.Integer] = com.google.common.collect.Ordering.natural().nullsLast()
    val strandNullsLast: Comparator[Strand] = com.google.common.collect.Ordering.natural().nullsLast()
    val stringNullsLast: Comparator[java.lang.String] = com.google.common.collect.Ordering.natural().nullsLast()
    // use ComparisonChain to safely handle nulls, as Feature is a java object
    ComparisonChain.start()
      // consider reference region first
      .compare(x.getContigName, y.getContigName)
      .compare(x.getStart, y.getStart)
      .compare(x.getEnd, y.getEnd)
      .compare(x.getStrand, y.getStrand, strandNullsLast)
      // then feature fields
      .compare(x.getFeatureId, y.getFeatureId, stringNullsLast)
      .compare(x.getFeatureType, y.getFeatureType, stringNullsLast)
      .compare(x.getName, y.getName, stringNullsLast)
      .compare(x.getSource, y.getSource, stringNullsLast)
      .compare(x.getPhase, y.getPhase, intNullsLast)
      .compare(x.getFrame, y.getFrame, intNullsLast)
      .compare(x.getScore, y.getScore, doubleNullsLast)
      // finally gene structure
      .compare(x.getGeneId, y.getGeneId, stringNullsLast)
      .compare(x.getTranscriptId, y.getTranscriptId, stringNullsLast)
      .compare(x.getExonId, y.getExonId, stringNullsLast)
      .compare(allowNull(x.getAttributes.get("exon_number")), allowNull(y.getAttributes.get("exon_number")), intNullsLast)
      .compare(allowNull(x.getAttributes.get("intron_number")), allowNull(y.getAttributes.get("intron_number")), intNullsLast)
      .compare(allowNull(x.getAttributes.get("rank")), allowNull(y.getAttributes.get("rank")), intNullsLast)
      .result()
  }
}
object FeatureOrdering extends FeatureOrdering[Feature] {}

class FeatureRDDFunctions(featureRDD: RDD[Feature]) extends ADAMSequenceDictionaryRDDAggregator[Feature](featureRDD) with Logging {

  def getSequenceRecords(elem: Feature): Set[SequenceRecord] = {
    Set(SequenceRecord(elem.getContigName, 1L))
  }

  private def strand(str: Strand): Boolean = str match {
    case Strand.FORWARD     => true
    case Strand.REVERSE     => false
    case Strand.INDEPENDENT => true
    case Strand.UNKNOWN     => true
  }

  def toGenes(): RDD[Gene] = {

    /*
    Creating a set of gene models works in four steps:

    1. Key each GTFFeature by its featureType (which should be either 'gene',
       'transcript', or 'exon').

    2. Take all the values of type 'exon', and create Exon objects from them.
       Key this RDD by transcriptId of each exon, and group all exons of the same
       transcript together. Also, do the same for the 'cds' features.

    3. Take all the values of type 'transcript', key them by their transcriptId,
       and join them with the cds and exons from step #2.  This should give each us
       enough information to create each Transcript, which we do, key each Transcript
       by its geneId, and group the transcripts which share a common gene together.

    4. Finally, find each 'gene'-typed GTFFeature, key it by its geneId,
       and join with the transcripts in #3.  Use these joined values to create the
       final set of Gene values.

    The three groupBys and two joins are necessary for creating the two-level hierarchical
    tree-structured Gene models, under the assumption that the rows of the GTF file itself
    aren't ordered.
     */

    // Step #1
    val typePartitioned: RDD[(String, Feature)] =
      featureRDD.keyBy(_.getFeatureType).cache()

    // Step #2
    val exonsByTranscript: RDD[(String, Iterable[Exon])] =
      typePartitioned.filter(_._1 == "exon").flatMap {
        // There really only should be _one_ parent listed in this flatMap, but since
        // getParentIds is modeled as returning a List[], we'll write it this way.
        case ("exon", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds
          ids.map(transcriptId => (
            transcriptId,
            Exon(ftr.getFeatureId, transcriptId, strand(ftr.getStrand), ReferenceRegion(ftr))
          ))
      }.groupByKey()

    val cdsByTranscript: RDD[(String, Iterable[CDS])] =
      typePartitioned.filter(_._1 == "CDS").flatMap {
        case ("CDS", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds
          ids.map(transcriptId => (
            transcriptId,
            CDS(transcriptId, strand(ftr.getStrand), ReferenceRegion(ftr))
          ))
      }.groupByKey()

    val utrsByTranscript: RDD[(String, Iterable[UTR])] =
      typePartitioned.filter(_._1 == "UTR").flatMap {
        case ("UTR", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds
          ids.map(transcriptId => (
            transcriptId,
            UTR(transcriptId, strand(ftr.getStrand), ReferenceRegion(ftr))
          ))
      }.groupByKey()

    // Step #3
    val transcriptsByGene: RDD[(String, Iterable[Transcript])] =
      typePartitioned.filter(_._1 == "transcript").map {
        case ("transcript", ftr: Feature) => (ftr.getFeatureId, ftr)
      }.join(exonsByTranscript)
        .leftOuterJoin(utrsByTranscript)
        .leftOuterJoin(cdsByTranscript)

        .flatMap {
          // There really only should be _one_ parent listed in this flatMap, but since
          // getParentIds is modeled as returning a List[], we'll write it this way.
          case (transcriptId: String, (((tgtf: Feature, exons: Iterable[Exon]),
            utrs: Option[Iterable[UTR]]),
            cds: Option[Iterable[CDS]])) =>
            val geneIds: Seq[String] = tgtf.getParentIds // should be length 1
            geneIds.map(geneId => (
              geneId,
              Transcript(transcriptId, Seq(transcriptId), geneId,
                strand(tgtf.getStrand),
                exons, cds.getOrElse(Seq()), utrs.getOrElse(Seq()))
            ))
        }.groupByKey()

    // Step #4
    val genes = typePartitioned.filter(_._1 == "gene").map {
      case ("gene", ftr: Feature) => (ftr.getFeatureId, ftr)
    }.leftOuterJoin(transcriptsByGene).map {
      case (geneId: String, (ggtf: Feature, transcripts: Option[Iterable[Transcript]])) =>
        Gene(geneId, Seq(geneId),
          strand(ggtf.getStrand),
          transcripts.getOrElse(Seq()))
    }

    genes
  }

  def reassignParentIds(): RDD[Feature] = {
    def reassignParentId(f: Feature): Feature = {
      val fb = Feature.newBuilder(f)

      val featureId: Option[String] = Option(f.getFeatureId)
      val geneId: Option[String] = Option(f.getGeneId)
      val transcriptId: Option[String] = Option(f.getTranscriptId)
      val exonNumber: Option[String] = Option(f.getAttributes.get("exon_number"))
      val exonId: Option[String] = Option(f.getExonId) match {
        case Some(e) => Option(e)
        case None    => transcriptId.flatMap(t => exonNumber.map(e => t + "_" + e))
      }

      val (featureIdOpt, parentIdOpt) =
        f.getFeatureType match {
          case "gene"        => (geneId, None)
          case "transcript"  => (transcriptId, geneId)
          case "exon"        => (exonId, transcriptId)
          case "CDS" | "UTR" => (featureId, transcriptId)
          case _             => (featureId, None)
        }
      // replace featureId and parentId if defined
      featureIdOpt.foreach(fb.setFeatureId)
      parentIdOpt.foreach(parentId => fb.setParentIds(List[String](parentId)))

      fb.build()
    }
    featureRDD.map(reassignParentId)
  }

  def filterByOverlappingRegion(query: ReferenceRegion): RDD[Feature] = {
    def overlapsQuery(rec: Feature): Boolean =
      rec.getContigName == query.referenceName &&
        rec.getStart < query.end &&
        rec.getEnd > query.start
    featureRDD.filter(overlapsQuery)
  }

  def saveAsGtf(fileName: String) = {
    def escape(entry: (Any, Any)): String = {
      entry._1 + " \"" + entry._2 + "\""
    }

    def toGtf(feature: Feature): String = {
      val seqname = feature.getContigName
      val source = Option(feature.getSource).getOrElse(".")
      val featureType = Option(feature.getFeatureType).getOrElse(".")
      val start = feature.getStart + 1 // GTF/GFF ranges are 1-based
      val end = feature.getEnd // GTF/GFF ranges are closed
      val score = Option(feature.getScore).getOrElse(".")
      val strand = Features.asString(feature.getStrand)
      val frame = Option(feature.getFrame).getOrElse(".")
      val attributes = Features.gatherAttributes(feature).map(escape).mkString("; ")
      List(seqname, source, featureType, start, end, score, strand, frame, attributes).mkString("\t")
    }
    featureRDD.map(toGtf).saveAsTextFile(fileName)
  }

  def saveAsGff3(fileName: String) = {
    def escape(entry: (Any, Any)): String = {
      entry._1 + "=" + entry._2
    }

    def toGff3(feature: Feature): String = {
      val seqid = feature.getContigName
      val source = Option(feature.getSource).getOrElse(".")
      val featureType = Option(feature.getFeatureType).getOrElse(".")
      val start = feature.getStart + 1 // GFF3 coordinate system is 1-based
      val end = feature.getEnd // GFF3 ranges are closed
      val score = Option(feature.getScore).getOrElse(".")
      val strand = Features.asString(feature.getStrand)
      val phase = Option(feature.getPhase).getOrElse(".")
      val attributes = Features.gatherAttributes(feature).map(escape).mkString(";")
      List(seqid, source, featureType, start, end, score, strand, phase, attributes).mkString("\t")
    }
    featureRDD.map(toGff3).saveAsTextFile(fileName)
  }

  def saveAsBed(fileName: String) = {
    def toBed(feature: Feature): String = {
      val chrom = feature.getContigName
      val start = feature.getStart
      val end = feature.getEnd
      val name = Features.nameOf(feature)
      val score = Option(feature.getScore).getOrElse(".")
      val strand = Features.asString(feature.getStrand)

      if (!feature.getAttributes.containsKey("thickStart")) {
        // write BED6 format
        List(chrom, start, end, name, score, strand).mkString("\t")
      } else {
        // write BED12 format
        val thickStart = feature.getAttributes.getOrElse("thickStart", ".")
        val thickEnd = feature.getAttributes.getOrElse("thickEnd", ".")
        val itemRgb = feature.getAttributes.getOrElse("itemRgb", ".")
        val blockCount = feature.getAttributes.getOrElse("blockCount", ".")
        val blockSizes = feature.getAttributes.getOrElse("blockSizes", ".")
        val blockStarts = feature.getAttributes.getOrElse("blockStarts", ".")
        List(chrom, start, end, name, score, strand, thickStart, thickEnd, itemRgb, blockCount, blockSizes, blockStarts).mkString("\t")
      }
    }
    featureRDD.map(toBed).saveAsTextFile(fileName)
  }

  def saveAsIntervalList(fileName: String) = {
    def toInterval(feature: Feature): String = {
      val sequenceName = feature.getContigName
      val start = feature.getStart + 1 // IntervalList ranges are 1-based
      val end = feature.getEnd // IntervalList ranges are closed
      val strand = Features.asString(feature.getStrand)
      val intervalName = Features.nameOf(feature)
      List(sequenceName, start, end, strand, intervalName).mkString("\t")
    }
    // todo:  SAM style header
    featureRDD.map(toInterval).saveAsTextFile(fileName)
  }

  def saveAsNarrowPeak(fileName: String) {
    def toNarrowPeak(feature: Feature): String = {
      val chrom = feature.getContigName
      val start = feature.getStart
      val end = feature.getEnd
      val name = Features.nameOf(feature)
      val score = Option(feature.getScore).getOrElse(".")
      val strand = Features.asString(feature.getStrand)
      val signalValue = feature.getAttributes.getOrElse("signalValue", "0")
      val pValue = feature.getAttributes.getOrElse("pValue", "-1")
      val qValue = feature.getAttributes.getOrElse("qValue", "-1")
      val peak = feature.getAttributes.getOrElse("peak", "-1")
      List(chrom, start, end, name, score, strand, signalValue, pValue, qValue, peak).mkString("\t")
    }
    featureRDD.map(toNarrowPeak).saveAsTextFile(fileName)
  }

  def sortByReference(ascending: Boolean = true, numPartitions: Int = featureRDD.partitions.length): RDD[Feature] = {
    implicit def ord = FeatureOrdering
    featureRDD.sortBy(f => f, ascending, numPartitions)
  }
}
