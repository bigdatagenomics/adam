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
package org.bdgenomics.adam.sql

import org.bdgenomics.adam.models.{
  ReferencePosition,
  VariantContext => VariantContextModel
}
import org.bdgenomics.adam.rich.RichVariant

object VariantContext {

  def fromModel(vc: VariantContextModel): VariantContext = {
    VariantContext(vc.position.referenceName,
      vc.position.start,
      vc.variant.variant.getEnd,
      Variant.fromAvro(vc.variant.variant),
      vc.genotypes.map(g => Genotype.fromAvro(g)).toSeq)
  }
}

case class VariantContext(referenceName: String,
                          start: Long,
                          end: Long,
                          variant: Variant,
                          genotypes: Seq[Genotype]) {

  def toModel(): VariantContextModel = {
    new VariantContextModel(new ReferencePosition(referenceName, start),
      RichVariant(variant.toAvro),
      genotypes.map(_.toAvro))
  }
}
