package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.DecadentRead
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._
import edu.berkeley.cs.amplab.adam.rich.ReferenceLocation
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.immutable._
import scala.collection.mutable
import java.io.File

class SnpTable(private val table: Map[String, Set[Long]]) extends Serializable with Logging {
  log.info("SNP table has %s contigs and %s entries".format(table.size, table.values.map(_.size).sum))

  /**
   * Is there a known SNP at the reference location of this Residue?
   */
  def isMasked(residue: Residue): Boolean =
    isMasked(residue.referenceLocation)

  /**
   * Is there a known SNP at the given reference location?
   */
  def isMasked(location: ReferenceLocation): Boolean = {
    val bucket = table.get(location.contig)
    if(bucket.isEmpty) unknownContigWarning(location.contig)
    bucket.map(_.contains(location.offset)).getOrElse(false)
  }

  private val unknownContigs = new mutable.HashSet[String]

  private def unknownContigWarning(contig: String) = {
    // This is synchronized to avoid a data race. Multiple threads may
    // race to update `unknownContigs`, e.g. when running with a Spark
    // master of `local[N]`.
    synchronized {
      if(!unknownContigs.contains(contig)) {
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
    val tuples = lines.filter(line => !line.startsWith("#")).map(line => {
      val split = line.split("\t")
      val contig = split(0)
      val pos = split(1).toLong - 1
      assert(pos >= 0)
      (contig, pos)
    })
    // construct map from contig to set of positions
    // this is done in-place to reduce overhead
    val table = new mutable.HashMap[String, mutable.HashSet[Long]]
    tuples.foreach(tup => table.getOrElseUpdate(tup._1, { new mutable.HashSet[Long] }) += tup._2)
    // construct SnpTable from immutable copy of `table`
    new SnpTable(table.mapValues(_.toSet).toMap)
  }
}
