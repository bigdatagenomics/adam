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
package org.bdgenomics.adam.projections

import com.google.common.collect.ImmutableList
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.FeatureField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Dbxref,
  Feature,
  OntologyTerm,
  Strand
}

class FeatureFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet features") {
    val path = tmpFile("features.parquet")
    val rdd = sc.parallelize(Seq(Feature.newBuilder()
      .setFeatureId("ENSG00000206503")
      .setName("HLA-A")
      .setSource("Ensembl")
      .setFeatureType("gene")
      .setContigName("6")
      .setStart(29941260L)
      .setEnd(29945884L)
      .setStrand(Strand.FORWARD)
      .setPhase(1)
      .setFrame(2)
      .setScore(1.0f)
      .setGeneId("ENSG00000206503")
      .setTranscriptId("ENST00000396634.5")
      .setExonId("ENSE00001677386")
      .setAliases(ImmutableList.of("alias"))
      .setParentIds(ImmutableList.of("parent_id"))
      .setTarget("target")
      .setGap("gap")
      .setDerivesFrom("derives_from")
      .setNotes(ImmutableList.of("note"))
      .setDbxrefs(ImmutableList.of(Dbxref.newBuilder()
        .setDb("EMBL")
        .setAccession("AA816246")
        .build()))
      .setOntologyTerms(ImmutableList.of(OntologyTerm.newBuilder()
        .setDb("GO")
        .setAccession("0046703")
        .build()))
      .setCircular(true)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      featureId,
      name,
      source,
      featureType,
      contigName,
      start,
      end,
      strand,
      phase,
      frame,
      score,
      geneId,
      transcriptId,
      exonId,
      aliases,
      parentIds,
      target,
      gap,
      derivesFrom,
      notes,
      dbxrefs,
      ontologyTerms,
      circular,
      attributes
    )

    val features: RDD[Feature] = sc.loadParquet(path, projection = Some(projection))
    assert(features.count() === 1)
    assert(features.first.getFeatureId === "ENSG00000206503")
    assert(features.first.getName === "HLA-A")
    assert(features.first.getSource === "Ensembl")
    assert(features.first.getFeatureType === "gene")
    assert(features.first.getContigName === "6")
    assert(features.first.getStart === 29941260L)
    assert(features.first.getEnd === 29945884L)
    assert(features.first.getStrand === Strand.FORWARD)
    assert(features.first.getPhase === 1)
    assert(features.first.getFrame === 2)
    assert(features.first.getScore === 1.0f)
    assert(features.first.getGeneId === "ENSG00000206503")
    assert(features.first.getTranscriptId === "ENST00000396634.5")
    assert(features.first.getExonId === "ENSE00001677386")
    assert(features.first.getAliases.get(0) === "alias")
    assert(features.first.getParentIds.get(0) === "parent_id")
    assert(features.first.getTarget === "target")
    assert(features.first.getGap === "gap")
    assert(features.first.getDerivesFrom === "derives_from")
    assert(features.first.getNotes.get(0) === "note")
    assert(features.first.getDbxrefs.get(0).getAccession === "AA816246")
    assert(features.first.getOntologyTerms.get(0).getAccession === "0046703")
    assert(features.first.getCircular === true)
  }
}
