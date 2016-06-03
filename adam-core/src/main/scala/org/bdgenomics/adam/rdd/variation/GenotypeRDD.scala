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
package org.bdgenomics.adam.rdd.variation

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.MultisampleAvroGenomicRDD
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.formats.avro.{ Contig, Genotype }

case class GenotypeRDD(rdd: RDD[Genotype],
                       sequences: SequenceDictionary,
                       samples: Seq[String]) extends MultisampleAvroGenomicRDD[Genotype] {

  def filterByOverlappingRegion(query: ReferenceRegion): RDD[Genotype] = {
    def overlapsQuery(rec: Genotype): Boolean =
      rec.getContigName == query.referenceName &&
        rec.getStart < query.end &&
        rec.getEnd > query.start
    rdd.filter(overlapsQuery)
  }

  def toVariantContextRDD: VariantContextRDD = {
    val vcIntRdd: RDD[(RichVariant, Genotype)] = rdd.keyBy(g => {
      RichVariant.genotypeToRichVariant(g)
    })
    val vcRdd = vcIntRdd.groupByKey
      .map { case (v: RichVariant, g) => new VariantContext(ReferencePosition(v), v, g, None) }

    VariantContextRDD(vcRdd, sequences, samples)
  }

  def save(args: SaveArgs): Boolean = {
    maybeSaveVcf(args) || {
      saveAsParquet(args); true
    }
  }

  def saveAsVcf(args: SaveArgs,
                sortOnSave: Boolean = false) {
    toVariantContextRDD.saveAsVcf(args, sortOnSave)
  }

  private def maybeSaveVcf(args: SaveArgs): Boolean = {
    if (args.outputPath.endsWith(".vcf")) {
      saveAsVcf(args)
      true
    } else {
      false
    }
  }
}
