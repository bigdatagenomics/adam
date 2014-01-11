package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.immutable._
import scala.collection.mutable
import java.io.File

class SnpTable(private val table: Map[String, Set[Long]]) extends Serializable with Logging {
  log.info("SNP table has %s contigs and %s entries".format(table.size, table.values.map(_.size).sum))

  def isMaskedAtReadOffset(read: ADAMRecord, offset: Int): Boolean = {
    val position = read.readOffsetToReferencePosition(offset)
    try {
      position.isEmpty || table(read.getReferenceName.toString).contains(position.get)
    } catch {
      case e: java.util.NoSuchElementException =>
        false
    }
  }
}

object SnpTable {
  def apply(): SnpTable = {
    new SnpTable(Map[String, Set[Long]]())
  }

  // `dbSNP` is expected to be a sites-only VCF
  def apply(dbSNP: File): SnpTable = {
    // parse into tuples of (contig, position)
    val lines = scala.io.Source.fromFile(dbSNP).getLines()
    val tuples = lines.filter(line => !line.startsWith("#")).map(line => {
      val split = line.split("\t")
      val contig = split(0)
      val pos = split(1).toLong
      (contig, pos)
    })
    // construct map from contig to set of positions
    // this is done in-place to reduce overhead
    val table = new mutable.HashMap[String, mutable.HashSet[Long]]
    tuples.foreach(tup => table.getOrElseUpdate(tup._1, { new mutable.HashSet[Long] }) += tup._2)
    // construct SnpTable from immutable copy of `table`
    new SnpTable(table.mapValues(_.toSet).toMap)
  }

  /*
  def apply(lines: RDD[String]): SnpTable = {
    // parse into tuples of (contig, position)
    val tuples = lines.filter(line => !line.startsWith("#")).map(line => {
      val split = line.split("\t")
      val contig = split(0)
      val pos = split(1).toLong
      (contig, pos)
    })
    // construct map from contig to set of positions
    val table = tuples.groupByKey.collect.toMap.mapValues(_.toSet)
    new SnpTable(table)
  }
  */
}
