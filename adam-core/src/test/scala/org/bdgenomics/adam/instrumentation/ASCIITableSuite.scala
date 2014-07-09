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

import java.io.{ PrintStream, ByteArrayOutputStream }
import org.scalatest.FunSuite

class ASCIITableSuite extends FunSuite {

  test("Table is rendered correctly") {

    val header = Array(
      new ASCIITableHeader(name = "Col1", alignment = Alignment.Left),
      new ASCIITableHeader(name = "Col2", alignment = Alignment.Right),
      new ASCIITableHeader(name = "Col3 Really Quite A Long Column", alignment = Alignment.Center))

    val rows = Array[Array[String]](
      Array("Col1Value Longer", "Col2Value1", "A"),
      Array("Col1Value", "Col2Value2 A Bit Longer", "BBBBB"))

    val asciiTable = new ASCIITable(header, rows)

    val renderedTable = getRenderedTable(asciiTable)

    assert(renderedTable === expectedRendering)

  }

  test("Inequal sizes between header and rows are rejected") {

    val header = Array(
      new ASCIITableHeader(name = "Col1", alignment = Alignment.Left),
      new ASCIITableHeader(name = "Col2", alignment = Alignment.Right))

    // The second row has 3 values, whereas the header only has two
    val rows = Array[Array[String]](
      Array("Col1Value Longer", "Col2Value1"),
      Array("Col1Value", "Col2Value2 A Bit Longer", "BBBBB"))

    intercept[IllegalArgumentException] {
      new ASCIITable(header, rows)
    }

  }

  private def getRenderedTable(table: ASCIITable): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new PrintStream(bytes)
    table.print(out)
    bytes.toString("UTF8")
  }

  def expectedRendering =
    """+------------------+-------------------------+---------------------------------+
      #|       Col1       |          Col2           | Col3 Really Quite A Long Column |
      #+------------------+-------------------------+---------------------------------+
      #| Col1Value Longer |              Col2Value1 |                A                |
      #| Col1Value        | Col2Value2 A Bit Longer |              BBBBB              |
      #+------------------+-------------------------+---------------------------------+
      #""".stripMargin('#')

}
