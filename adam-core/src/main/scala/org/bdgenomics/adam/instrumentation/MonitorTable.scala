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
package org.bdgenomics.adam.instrumentation

import com.netflix.servo.monitor.{ CompositeMonitor, Monitor }
import com.netflix.servo.tag.Tag
import java.io.PrintStream
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Tabular representation of a set of [[Monitor]]s. A [[MonitorTable]] consists of a list of [[TableHeader]]s, which specify not only the
 * titles of the columns, but also how the data is extracted from the rows, as well as a list of rows which are [[Monitor]]s containing the data.
 * Each header can specify the following:
 *
 * - name: the name of the column; this will be the text that is used in the header row
 * - valueExtractor: specifies how data will be extracted from a monitor; the functions in the [[ValueExtractor]] object can be used to create
 * [[ValueExtractor]]s for a particular purpose
 * - formatFunction (optional): a function which formats the value extracted from the monitor as a [[String]]
 * - alignment (optional): specifies how the data in a cell is aligned; defaults to right-alignment
 */
class MonitorTable(headerRow: Array[TableHeader], rows: Array[Monitor[_]]) {

  def print(out: PrintStream) = {
    val tableHeader = headerRow.map(e => { new ASCIITableHeader(e.name, e.alignment) })
    val tableRows = createRows()
    val table = new ASCIITable(tableHeader, tableRows)
    table.print(out)
  }

  private def createRows(): Array[Array[String]] = {
    rows.map(row => {
      val columns = new ArrayBuffer[String]
      headerRow.foreach(headerCol => {
        val valueOption = headerCol.valueExtractor.extractValue(row)
        columns += stringValue(headerCol, valueOption)
      })
      columns.toArray
    })
  }

  private def stringValue(headerCol: TableHeader, option: Option[Any]): String = {
    option.foreach(value => {
      if (headerCol.formatFunction.isDefined)
        return headerCol.formatFunction.get.apply(value)
      else
        return value.toString
    })
    "-"
  }

}

/**
 * Specifies the title of a column, as well as how data is extracted to form the cells of this column
 */
case class TableHeader(name: String, valueExtractor: ValueExtractor, formatFunction: Option[(Any) => String] = None,
                       alignment: Alignment.Alignment = Alignment.Right)

object ValueExtractor {
  /**
   * Creates a [[ValueExtractor]] which extracts values from [[Monitor]]s matching the specified tag.
   * If the monitor is a [[CompositeMonitor]] and one of its sub-monitors matches the tag then the value of this monitor is returned.
   * If the monitor is not a [[CompositeMonitor]], then if the value of the monitor itself matches the tag, the value of the monitor is returned
   */
  def forMonitorMatchingTag(tag: Tag): ValueExtractor = {
    new CompositeMonitorValueExtractor(tag)
  }

  /**
   * Creates a [[ValueExtractor]] which returns the value of the tag with the specified key from the monitor
   */
  def forTagValueWithKey(key: String): ValueExtractor = {
    new MonitorTagValueExtractor(key)
  }

  /**
   * Creates a [[ValueExtractor]] which returns the value of the specified monitor
   */
  def forMonitorValue(): ValueExtractor = {
    new SimpleMonitorValueExtractor()
  }
}

trait ValueExtractor {
  def extractValue(monitor: Monitor[_]): Option[Any]
}

private class SimpleMonitorValueExtractor() extends ValueExtractor {
  override def extractValue(monitor: Monitor[_]): Option[Any] = {
    Option(monitor.getValue)
  }
}

private class MonitorTagValueExtractor(key: String) extends ValueExtractor {
  override def extractValue(monitor: Monitor[_]): Option[Any] = {
    Option(monitor.getConfig.getTags.getValue(key))
  }
}

private class CompositeMonitorValueExtractor(tag: Tag) extends ValueExtractor {

  override def extractValue(monitor: Monitor[_]): Option[Any] = {
    val matching = findMatchingMonitor(monitor)
    if (matching.isDefined) Option(matching.get.getValue) else None
  }

  private def findMatchingMonitor(monitor: Monitor[_]): Option[Monitor[_]] = {
    monitor match {
      case monitor: CompositeMonitor[_] =>
        monitor.getMonitors.foreach(subMonitor => {
          val matching = returnMonitorIfMatching(subMonitor)
          if (matching.isDefined) {
            return matching
          }
        })
        None
      case _ =>
        // If this monitor matches the tag from the headerCol, return it
        returnMonitorIfMatching(monitor)
    }
  }

  private def returnMonitorIfMatching(monitor: Monitor[_]): Option[Monitor[_]] = {
    val tagValue = monitor.getConfig.getTags.getValue(tag.getKey)
    if (tagValue != null && tagValue.equals(tag.getValue)) {
      Some(monitor)
    } else {
      None
    }
  }

}
