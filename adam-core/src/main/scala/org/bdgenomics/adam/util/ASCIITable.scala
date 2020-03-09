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
package org.bdgenomics.adam.util

import java.io.PrintWriter
import org.apache.commons.lang.StringUtils
import org.bdgenomics.adam.util.TextAlignment.TextAlignment

/**
 * Renders a table using ASCII characters. The table is represented by a header row, consisting of [[ASCIITableHeader]] objects
 * which define the column label and the column alignment, and multiple rows, each of which contains an array of values to go in each row.
 *
 * There must be an equal number of items in each row and in the header.
 */
private[util] class ASCIITable(header: Array[ASCIITableHeader], rows: Array[Array[String]]) {

  rows.foreach(row => {
    if (row.length != header.length) {
      throw new IllegalArgumentException("Row length [" + row.length +
        "] did not match header length [" + header.length + "]")
    }
  })

  override def toString: String = {
    val columnWidths = new Array[Int](header.length)
    updateColumnWidths(columnWidths, header.map(_.name))
    rows.foreach(row => {
      updateColumnWidths(columnWidths, row)
    })
    "%s\n".format(Seq(separatorToString(columnWidths),
      rowToString(header.map(_.name),
        Array.fill(header.length)(TextAlignment.Center),
        columnWidths),
      separatorToString(columnWidths),
      rowsToString(rows, header, columnWidths),
      separatorToString(columnWidths)).mkString("\n"))
  }

  private def separatorToString(columnWidths: Array[Int]): String = {
    "+%s+".format(columnWidths.map(columnWidth => {
      StringUtils.repeat("-", getRealColumnWidth(columnWidth))
    }).mkString("+"))
  }

  private def rowsToString(rows: Array[Array[String]],
                           header: Array[ASCIITableHeader],
                           columnWidths: Array[Int]): String = {
    rows.map(row => {
      rowToString(row, header.map(_.alignment), columnWidths)
    }).mkString("\n")
  }

  private def rowToString(row: Array[String],
                          alignments: Array[TextAlignment],
                          columnWidths: Array[Int]): String = {
    var i = 0
    "|%s|".format(row.map(column => {
      val columnWidth = getRealColumnWidth(columnWidths(i))
      val col = paddedValue(column, columnWidth, alignments(i))
      i += 1
      col
    }).mkString("|"))
  }

  private def getRealColumnWidth(columnWidth: Int): Int = {
    columnWidth + 2
  }

  private def paddedValue(value: String, width: Int, alignment: TextAlignment): String = {
    alignment match {
      case TextAlignment.Left   => " " + StringUtils.rightPad(value, width - 1, ' ')
      case TextAlignment.Right  => StringUtils.leftPad(value, width - 1, ' ') + " "
      case TextAlignment.Center => StringUtils.center(value, width, ' ')
    }
  }

  private def updateColumnWidths(columnWidths: Array[Int], row: Array[String]) = {
    for (i <- columnWidths.indices) {
      if (row(i).length > columnWidths(i)) {
        columnWidths(i) = row(i).length
      }
    }
  }
}

private[util] case class ASCIITableHeader(name: String, alignment: TextAlignment = TextAlignment.Right)
