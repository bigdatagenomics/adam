/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
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

package edu.berkeley.cs.amplab.adam.cli

import collection.mutable.{ HashMap, MutableList }
import java.io._
import org.apache.spark.Logging
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

/*
 * These classes are meant to be used to do simple performance
 * measurements across ADAM.
 */
object TimingAggregator extends Logging {
  val timingAggregators : ThreadLocal[HashMap[String, TimingAggregator]] = new ThreadLocal[HashMap[String, TimingAggregator]]() {
    override def initialValue() : HashMap[String, TimingAggregator] = {
      return new HashMap[String, TimingAggregator]()
    }
  }

  def Start(phaseName : String) {
    val t = new TimingAggregator(phaseName)
    timingAggregators.get += (phaseName -> t)
  }

  def Stop(phaseName : String) {
    Stop(phaseName, "")
  }

  def Stop(phaseName : String, note : String) {
    timingAggregators.get()(phaseName).Stop(note)
  }

  def Flush(logFile : String) {
    val writer = new FileWriter(logFile, /* append */ true)
    timingAggregators.get.values.foreach(timing => {
      writer.write(List(timing.phaseName,s"${timing.start}",s"${timing.end}",s"${timing.note}").filter(!_.isEmpty).mkString(",") + "\n")
    })
    writer.close
    Reset
  }

  def Reset() {
    timingAggregators.get.clear
  }
}

class TimingAggregator(val phaseName : String) {
  val start : Long = (new java.util.Date()).getTime
  var end : Long = -1
  var note : String = ""

  def Stop(note : String) {
    end = (new java.util.Date()).getTime
    this.note = note
  }
}
