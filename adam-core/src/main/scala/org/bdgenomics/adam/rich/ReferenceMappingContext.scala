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
package org.bdgenomics.adam.rich

import org.bdgenomics.adam.models.{ ReferenceRegion, ReferenceMapping }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype }

/**
 * A common location in which to drop some ReferenceMapping implementations.
 */
object ReferenceMappingContext {

  implicit object GenotypeReferenceMapping extends ReferenceMapping[Genotype] with Serializable {
    override def getReferenceName(value: Genotype): String = value.getVariant.getContig.getContigName.toString
    override def getReferenceRegion(value: Genotype): ReferenceRegion =
      ReferenceRegion(value.getVariant.getContig.getContigName.toString, value.getVariant.getStart, value.getVariant.getEnd)
  }

  implicit object AlignmentRecordReferenceMapping extends ReferenceMapping[AlignmentRecord] with Serializable {
    override def getReferenceName(value: AlignmentRecord): String = value.getContig.getContigName.toString
    override def getReferenceRegion(value: AlignmentRecord): ReferenceRegion = ReferenceRegion(value).orNull
  }

  implicit object ReferenceRegionReferenceMapping extends ReferenceMapping[ReferenceRegion] with Serializable {
    override def getReferenceName(value: ReferenceRegion): String = value.referenceName.toString
    override def getReferenceRegion(value: ReferenceRegion): ReferenceRegion = value
  }

  implicit object FeatureReferenceMapping extends ReferenceMapping[Feature] with Serializable {
    override def getReferenceName(value: Feature): String = value.getContig.getContigName.toString
    override def getReferenceRegion(value: Feature): ReferenceRegion = ReferenceRegion(value)
  }
}
