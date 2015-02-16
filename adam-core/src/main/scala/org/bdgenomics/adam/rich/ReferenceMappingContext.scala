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
import org.bdgenomics.formats.avro.{ FlatGenotype, AlignmentRecord, Feature }

/**
 * A common location in which to drop some ReferenceMapping implementations.
 */
object ReferenceMappingContext {

  implicit object FlatGenotypeReferenceMapping extends ReferenceMapping[FlatGenotype] with Serializable {
    override def getReferenceName(value: FlatGenotype): String = value.getReferenceName.toString
    override def getReferenceRegion(value: FlatGenotype): ReferenceRegion =
      ReferenceRegion(value.getReferenceName.toString, value.getPosition, value.getPosition)
    override def hasReferenceRegion(value: FlatGenotype): Boolean =
      value.getReferenceName != null && value.getPosition != null
  }

  implicit object AlignmentRecordReferenceMapping extends ReferenceMapping[AlignmentRecord] with Serializable {
    override def getReferenceName(value: AlignmentRecord): String = value.getContig.getContigName.toString
    override def getReferenceRegion(value: AlignmentRecord): ReferenceRegion = ReferenceRegion(value).orNull
    override def hasReferenceRegion(value: AlignmentRecord): Boolean = value.getReadMapped
  }

  implicit object ReferenceRegionReferenceMapping extends ReferenceMapping[ReferenceRegion] with Serializable {
    override def getReferenceName(value: ReferenceRegion): String = value.referenceName.toString
    override def getReferenceRegion(value: ReferenceRegion): ReferenceRegion = value
    override def hasReferenceRegion(value: ReferenceRegion): Boolean = true
  }

  implicit object FeatureReferenceMapping extends ReferenceMapping[Feature] with Serializable {
    override def getReferenceName(value: Feature): String = value.getContig.getContigName.toString
    override def getReferenceRegion(value: Feature): ReferenceRegion = ReferenceRegion(value)
    override def hasReferenceRegion(value: Feature): Boolean =
      value.getContig.getContigName != null && value.getStart != null && value.getEnd != null
  }
}
