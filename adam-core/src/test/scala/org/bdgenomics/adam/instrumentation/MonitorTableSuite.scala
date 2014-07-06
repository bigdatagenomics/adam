/**
 * Copyright (c) 2014. Neil Ferguson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.scalatest.FunSuite
import com.netflix.servo.monitor.{ Monitor, MonitorConfig }
import com.netflix.servo.tag.Tags
import java.io.{ PrintStream, ByteArrayOutputStream }

class MonitorTableSuite extends FunSuite {

  test("Table is rendered correctly") {

    val headers = Array(
      new Header(name = "Col1", valueExtractor = ValueExtractor.forTagValueWithKey("TagKey1"), None, alignment = Alignment.Left),
      new Header(name = "Col2", valueExtractor = ValueExtractor.forMonitorMatchingTag(ServoTimer.TotalTimeTag), formatFunction = Some(formatFunction1)))

    val rows = Array[Monitor[_]](
      new ServoTimer(MonitorConfig.builder("timer1").build()),
      new ServoTimer(MonitorConfig.builder("timer2").build()))

    rows(0).asInstanceOf[ServoTimer].addTag(Tags.newTag("TagKey1", "Col1Value1"))
    rows(1).asInstanceOf[ServoTimer].addTag(Tags.newTag("TagKey1", "Col1Value2 A Bit Longer"))

    rows(0).asInstanceOf[ServoTimer].recordNanos(100)
    rows(1).asInstanceOf[ServoTimer].recordNanos(200000)

    val monitorTable = new MonitorTable(headers, rows)

    val renderedTable = getRenderedTable(monitorTable)

    assert(renderedTable === expectedTable)

  }

  private def getRenderedTable(table: MonitorTable): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new PrintStream(bytes)
    table.print(out)
    bytes.toString("UTF8")
  }

  private def formatFunction1(value: Any): String = {
    value.toString + " nanoseconds"
  }

  val expectedTable =
    """+-------------------------+--------------------+
      #|           Col1          |        Col2        |
      #+-------------------------+--------------------+
      #| Col1Value1              |    100 nanoseconds |
      #| Col1Value2 A Bit Longer | 200000 nanoseconds |
      #+-------------------------+--------------------+
      #""".stripMargin('#')

}
