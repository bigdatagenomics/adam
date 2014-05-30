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
package org.bdgenomics.adam.parquet_reimpl.index

import java.io._
import scala.io.Source
import scala.Some
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.io.{ FileLocator, ByteAccess }

/**
 * And identifier-range index is an index on both genomic ranges and sample identifiers simultaneously --
 * a pair of (sample-identifier, genomic-range) is associated with a single row group of a Parquet file
 * in a single IDRangeIndexEntry, and a set of IDRangeIndexEntry values are collected (in memory) into an
 * IDRangeIndex.
 */
class IDRangeIndex(val entries: Array[IDRangeIndexEntry]) extends RowGroupIndex[IDRangeIndexEntry] {
  def this(itr: Iterator[IDRangeIndexEntry]) = this(itr.toArray)
  def this(itr: Iterable[IDRangeIndexEntry]) = this(itr.toArray)
  def this(is: InputStream) = this(Source.fromInputStream(is).getLines().map(IDRangeIndex.parseIDRangeIndexEntry))
  def this(file: File) = this(new FileInputStream(file))
  def this(io: ByteAccess) = this(io.readByteStream(0, io.length().toInt))
  def this(io: FileLocator) = this(io.bytes)

  override def findIndexEntries(predicate: IndexEntryPredicate[IDRangeIndexEntry]): Iterable[IDRangeIndexEntry] = {
    entries.filter(predicate.accepts)
  }
}

class IDRangeIndexWriter(os: OutputStream) extends RowGroupIndexWriter[IDRangeIndexEntry] {
  def this(f: File) = this(new FileOutputStream(f))
  private val printer: PrintWriter = new PrintWriter(os)

  override def write(entry: IDRangeIndexEntry) {
    printer.println(entry.line)
  }
  override def close(): Unit = {
    printer.close()
  }

  def flush() {
    printer.flush()
  }
}

object IDRangeIndex {
  private val referenceRegionRegex = "([^:]+):(\\d+)-(\\d+)".r

  def parseRegion(regionString: String): ReferenceRegion = {
    referenceRegionRegex.findFirstMatchIn(regionString) match {
      case Some(m) => ReferenceRegion(m.group(1), m.group(2).toLong, m.group(3).toLong)
      case None    => throw new IllegalArgumentException("\"%s\" doesn't match reference region regex".format(regionString))
    }
  }

  def parseIDRangeIndexEntry(line: String): IDRangeIndexEntry = {
    val array = line.split("\t")
    val path = array(0)
    val index = array(1).toInt
    val id = array(2)
    val range = parseRegion(array(3))
    new IDRangeIndexEntry(path, index, id, range)
  }
}

case class IDRangeIndexPredicate(queryRange: Option[ReferenceRegion], queryIDs: Option[Set[String]] = None) extends IndexEntryPredicate[IDRangeIndexEntry] {
  def this(ref: ReferenceRegion) = this(Some(ref), None)

  def matchesQueryRange(entry: IDRangeIndexEntry): Boolean =
    queryRange match {
      case Some(range) => range.overlaps(entry.range)
      case None        => true
    }
  def matchesQueryIds(entry: IDRangeIndexEntry): Boolean =
    queryIDs match {
      case Some(idSet) => idSet.contains(entry.id)
      case None        => true
    }
  override def accepts(entry: IDRangeIndexEntry): Boolean =
    matchesQueryRange(entry) && matchesQueryIds(entry)
}

case class IDRangeIndexEntry(path: String, rowGroupIndex: Int, id: String, range: ReferenceRegion)
    extends RowGroupIndexEntry(path, rowGroupIndex) {

  def stringifyRange(range: ReferenceRegion): String = "%s:%d-%d".format(range.referenceName, range.start, range.end)

  def line: String = {
    "%s\t%d\t%s\t%s".format(path, rowGroupIndex, id, stringifyRange(range))
  }
}
