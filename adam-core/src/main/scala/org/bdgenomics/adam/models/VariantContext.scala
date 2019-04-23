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
package org.bdgenomics.adam.models

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.{ Genotype, Variant, VariantAnnotation }

/**
 * Singleton object for building VariantContexts.
 */
object VariantContext {

  /**
   * Create a new variant context from a variant.
   *
   * @param v Variant to create a variant context from.
   * @return Return a new variant context created from the specified variant.
   */
  def apply(v: Variant): VariantContext = {
    new VariantContext(ReferencePosition(v), RichVariant(v), Iterable.empty)
  }

  /**
   * Create a new variant context from a variant and one or more genotypes.
   *
   * @param v Variant to create a variant context from.
   * @param genotypes One or more genotypes.
   * @return Return a new variant context created from the specified variant
   *    and genotypes.
   */
  def apply(v: Variant, genotypes: Iterable[Genotype]): VariantContext = {
    new VariantContext(ReferencePosition(v), RichVariant(v), genotypes)
  }

  /**
   * Create a new variant context from a set of genotypes.
   *
   * @note Genotypes must be at the same position.
   *
   * @param genotypes One or more genotypes to create a variant context from.
   * @return Return a new variant context created from the specified set of
   *    genotypes.
   */
  def buildFromGenotypes(genotypes: Seq[Genotype]): VariantContext = {
    val position = ReferencePosition(genotypes.head)
    require(
      genotypes.map(ReferencePosition(_)).forall(_ == position),
      "Genotypes do not all have the same position."
    )

    val g = genotypes.head
    val variant = Variant.newBuilder()
      .setReferenceName(g.getReferenceName)
      .setStart(g.getStart)
      .setEnd(g.getEnd)
      .setSplitFromMultiallelic(g.getSplitFromMultiallelic)
      .setReferenceAllele(g.getReferenceAllele)
      .setAlternateAllele(g.getAlternateAllele)
      .build()

    new VariantContext(position, RichVariant(variant), genotypes)
  }
}

/**
 * A representation of all variation data at a single variant.
 *
 * This class represents an equivalent to a single allele from a VCF line, and
 * is the ADAM equivalent to htsjdk.variant.variantcontext.VariantContext.
 *
 * @param position The locus that the variant is at.
 * @param variant The variant allele that is contained in this VariantContext.
 * @param genotypes An iterable collection of Genotypes where this allele was
 *   called. Equivalent to the per-sample FORMAT fields in a VCF.
 */
class VariantContext(
    val position: ReferencePosition,
    val variant: RichVariant,
    val genotypes: Iterable[Genotype]) extends Serializable {
}

class VariantContextSerializer extends Serializer[VariantContext] {

  val rpSerializer = new ReferencePositionSerializer
  val vSerializer = new AvroSerializer[Variant]
  val gtSerializer = new AvroSerializer[Genotype]

  def write(kryo: Kryo, output: Output, obj: VariantContext) = {
    rpSerializer.write(kryo, output, obj.position)
    vSerializer.write(kryo, output, obj.variant.variant)
    output.writeInt(obj.genotypes.size)
    obj.genotypes.foreach(gt => gtSerializer.write(kryo, output, gt))
  }

  def read(kryo: Kryo, input: Input, klazz: Class[VariantContext]): VariantContext = {
    val rp = rpSerializer.read(kryo, input, classOf[ReferencePosition])
    val v = vSerializer.read(kryo, input, classOf[Variant])
    val gts = new Array[Genotype](input.readInt())
    gts.indices.foreach(idx => {
      gts(idx) = gtSerializer.read(kryo, input, classOf[Genotype])
    })
    new VariantContext(rp, new RichVariant(v), gts.toIterable)
  }
}
