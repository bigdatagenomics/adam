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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.models._
import org.bdgenomics.formats.avro.{ Strand, Feature }
import scala.collection.JavaConversions._

class FeatureRDDFunctions(featureRDD: RDD[Feature]) extends Serializable with Logging {

  private def strand(str: Strand): Boolean = str match {
    case Strand.Forward     => true
    case Strand.Reverse     => false
    case Strand.Independent => true
  }

  def asGenes(): RDD[Gene] = {

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
    and join with
    *
       the transcripts in #3.  Use these joined values to create the final set of
       Gene values.

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
          ids.map(transcriptId => (transcriptId,
            Exon(ftr.getFeatureId, transcriptId, strand(ftr.getStrand), ReferenceRegion(ftr))))
      }.groupByKey()

    val cdsByTranscript: RDD[(String, Iterable[CDS])] =
      typePartitioned.filter(_._1 == "CDS").flatMap {
        case ("CDS", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds
          ids.map(transcriptId => (transcriptId,
            CDS(transcriptId, strand(ftr.getStrand), ReferenceRegion(ftr))))
      }.groupByKey()

    val utrsByTranscript: RDD[(String, Iterable[UTR])] =
      typePartitioned.filter(_._1 == "UTR").flatMap {
        case ("UTR", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds
          ids.map(transcriptId => (transcriptId,
            UTR(transcriptId, strand(ftr.getStrand), ReferenceRegion(ftr))))
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
            geneIds.map(geneId => (geneId,
              Transcript(transcriptId, Seq(transcriptId), geneId,
                strand(tgtf.getStrand),
                exons, cds.getOrElse(Seq()), utrs.getOrElse(Seq()))))
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

  def filterByOverlappingRegion(query: ReferenceRegion): RDD[Feature] = {
    def overlapsQuery(rec: Feature): Boolean =
      rec.getContig.getContigName == query.referenceName &&
        rec.getStart < query.end &&
        rec.getEnd > query.start
    featureRDD.filter(overlapsQuery)
  }
}
