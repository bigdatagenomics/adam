/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.converters

import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.scalatest.FunSuite
import org.broadinstitute.variant.variantcontext.{Allele, VariantContextBuilder, GenotypeBuilder}
import java.lang.Integer
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.avro.ADAMGenotypeAllele
import edu.berkeley.cs.amplab.adam.avro.Base

class VariantContextConverterSuite extends FunSuite {
  val dictionary = SequenceDictionary(SequenceRecord(1, "chr1", 249250621, "file://ucsc.hg19.fasta", "1b22b98cdeb4a9304cb5d48026a85128"))

  def snvBuilder: VariantContextBuilder = new VariantContextBuilder()
    .alleles(List(Allele.create("A",true), Allele.create("T")).asJavaCollection)
    .start(1L)
    .stop(1L)
    .chr("chr1")

  test("Convert site-only SNV") {
    val vc = new VariantContextBuilder()
      .alleles(List(Allele.create("A",true), Allele.create("T")).asJavaCollection)
      .start(1L)
      .stop(1L)
      .chr("chr1")
      .make()

    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.length === 0)

    val variant = adamVC.variant

    val contig = variant.getContig
    assert(contig.getContigName === "chr1")
    assert(contig.getReferenceLength === 249250621)
    assert(contig.getReferenceURL === "file://ucsc.hg19.fasta")
    assert(contig.getReferenceMD5 === "1b22b98cdeb4a9304cb5d48026a85128")

    assert(variant.getReferenceAlleles.contains(Base.A))
    assert(variant.getPosition === 0L)
  }

  test("Convert genotypes with phase information") {
    val vcb = snvBuilder

    val genotypeAttributes = JavaConversions.mapAsJavaMap(Map[String, Object]("PQ" -> new Integer(50), "PS" -> "1"))
    val vc = vcb.genotypes(GenotypeBuilder.create("NA12878", vcb.getAlleles(), genotypeAttributes)).make()

    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)

    val adamGTs = adamVCs.flatMap(_.genotypes)
    assert(adamGTs.length === 1)
    val adamGT = adamGTs.head
    assert(adamGT.getAlleles.asScala.sameElements(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))
    assert(adamGT.getPhaseSetId === "1")
    assert(adamGT.getPhaseQuality === 50)
  }

  test("PASSing variants") {
    val vcb = snvBuilder
    vcb.genotypes(GenotypeBuilder.create("NA12878", vcb.getAlleles))
    vcb.passFilters()

    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(vcb.make)
    val adamGT = adamVCs.flatMap(_.genotypes).head

    assert(adamGT.getVarIsFiltered === false)
  }

  test("non PASSing variants") {
    val vcb = snvBuilder
    vcb.genotypes(GenotypeBuilder.create("NA12878", vcb.getAlleles))
    vcb.filter("LowMQ")

    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(vcb.make)
    val adamGT = adamVCs.flatMap(_.genotypes).head

    assert(adamGT.getVarIsFiltered === true)
    assert(adamGT.getVarFilters.asScala.sameElements(List("LowMQ")))
  }
}
