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

import java.io.PrintStream
import org.apache.commons.lang.StringUtils
import org.bdgenomics.adam.instrumentation.Alignment.Alignment

/**
 * Renders a table using ASCII characters. The table is represented by a header row, consisting of [[ASCIITableHeader]] objects
 * which define the column label and the column alignment, and multiple rows, each of which contains an array of values to go in each row.
 *
 * There must be an equal number of items in each row and in the header.
 */
class ASCIITable(header: Array[ASCIITableHeader], rows: Array[Array[String]]) {

  rows.foreach(row => {
    if (row.length != header.length) {
      throw new IllegalArgumentException("Row length [" + row.length +
        "] did not match header length [" + header.length + "]")
    }
  })

  /**
   * Prints this table to the specified [[PrintStream]]
   */
  def print(out: PrintStream) {
    val columnWidths = new Array[Int](header.length)
    updateColumnWidths(columnWidths, header.map(_.name))
    rows.foreach(row => {
      updateColumnWidths(columnWidths, row)
    })
    printSeparator(out, columnWidths)
    printRow(out, header.map(_.name), Array.fill(header.length)(Alignment.Center), columnWidths)
    printSeparator(out, columnWidths)
    printRows(out, rows, header, columnWidths)
    printSeparator(out, columnWidths)
  }

  private def printSeparator(out: PrintStream, columnWidths: Array[Int]) = {
    columnWidths.foreach(columnWidth => {
      out.print('+')
      out.print(StringUtils.repeat("-", getRealColumnWidth(columnWidth)))
    })
    out.println('+')
  }

  private def printRows(out: PrintStream, rows: Array[Array[String]], header: Array[ASCIITableHeader], columnWidths: Array[Int]) = {
    rows.foreach(row => {
      printRow(out, row, header.map(_.alignment), columnWidths)
    })
  }

  private def printRow(out: PrintStream, row: Array[String], alignments: Array[Alignment], columnWidths: Array[Int]) {
    var i = 0
    row.foreach(column => {
      out.print('|')
      val columnWidth = getRealColumnWidth(columnWidths(i))
      out.print(paddedValue(column, columnWidth, alignments(i)))
      i += 1
    })
    out.println('|')
  }

  private def getRealColumnWidth(columnWidth: Int): Int = {
    columnWidth + 2
  }

  private def paddedValue(value: String, width: Int, alignment: Alignment): String = {
    alignment match {
      case Alignment.Left   => " " + StringUtils.rightPad(value, width - 1, ' ')
      case Alignment.Right  => StringUtils.leftPad(value, width - 1, ' ') + " "
      case Alignment.Center => StringUtils.center(value, width, ' ')
    }
  }

  private def updateColumnWidths(columnWidths: Array[Int], row: Array[String]) = {
    for (i <- 0 to columnWidths.length - 1) {
      if (row(i).length > columnWidths(i)) {
        columnWidths(i) = row(i).length
      }
    }
  }

}

case class ASCIITableHeader(name: String, alignment: Alignment = Alignment.Right)
