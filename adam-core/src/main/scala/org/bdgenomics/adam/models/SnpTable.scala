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

import java.io.File

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.DecadentRead._
import org.bdgenomics.adam.rich.RichVariant

import scala.collection.immutable._
import scala.collection.mutable

class SnpTable(private val table: Map[String, Set[Long]]) extends Serializable with Logging {
  log.info("SNP table has %s contigs and %s entries".format(table.size, table.values.map(_.size).sum))

  /**
   * Is there a known SNP at the reference location of this Residue?
   */
  def isMasked(residue: Residue): Boolean =
    contains(residue.referencePosition)

  /**
   * Is there a known SNP at the given reference location?
   */
  def contains(location: ReferencePosition): Boolean = {
    val bucket = table.get(location.referenceName)
    if (bucket.isEmpty) unknownContigWarning(location.referenceName)
    bucket.map(_.contains(location.pos)).getOrElse(false)
  }

  private val unknownContigs = new mutable.HashSet[String]

  private def unknownContigWarning(contig: String) = {
    // This is synchronized to avoid a data race. Multiple threads may
    // race to update `unknownContigs`, e.g. when running with a Spark
    // master of `local[N]`.
    synchronized {
      if (!unknownContigs.contains(contig)) {
        unknownContigs += contig
        log.warn("Contig has no entries in known SNPs table: %s".format(contig))
      }
    }
  }
}

object SnpTable {
  def apply(): SnpTable = {
    new SnpTable(Map[String, Set[Long]]())
  }

  // `knownSnpsFile` is expected to be a sites-only VCF
  def apply(knownSnpsFile: File): SnpTable = {
    // parse into tuples of (contig, position)
    val lines = scala.io.Source.fromFile(knownSnpsFile).getLines()
    val tuples = lines.filter(line => !line.startsWith("#")).flatMap(line => {
      val split = line.split("\t")
      val contig = split(0)
      val pos = split(1).toLong - 1
      val ref = split(3)
      assert(pos >= 0)
      assert(!ref.isEmpty)
      ref.zipWithIndex.map {
        case (base, idx) =>
          assert(Seq('A', 'C', 'T', 'G', 'N').contains(base))
          (contig, pos + idx)
      }
    })
    // construct map from contig to set of positions
    // this is done in-place to reduce overhead
    val table = new mutable.HashMap[String, mutable.HashSet[Long]]
    tuples.foreach(tup => table.getOrElseUpdate(tup._1, { new mutable.HashSet[Long] }) += tup._2)
    // construct SnpTable from immutable copy of `table`
    new SnpTable(table.mapValues(_.toSet).toMap)
  }

  def apply(variants: RDD[RichVariant]): SnpTable = {
    val positions = variants.map(variant => (variant.getContig.getContigName.toString, variant.getStart)).collect()
    val table = new mutable.HashMap[String, mutable.HashSet[Long]]
    positions.foreach(tup => table.getOrElseUpdate(tup._1, { new mutable.HashSet[Long] }) += tup._2)
    new SnpTable(table.mapValues(_.toSet).toMap)
  }
}
