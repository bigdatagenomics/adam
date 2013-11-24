/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.util

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.adam.commands.SparkCommand
import java.net.ServerSocket

object SparkTest extends org.scalatest.Tag("edu.berkeley.cs.amplab.util.SparkFunSuite")

trait SparkFunSuite extends FunSuite {
  val sparkPortProperty = "spark.driver.port"
  var sc: SparkContext = _

  def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
    test(name, SparkTest) {
      // Use the same context properties as ADAM commands
      SparkCommand.setupContextProperties()
      // Silence the Spark logs if requested
      val maybeLevels = if (silenceSpark) Some(SparkLogUtil.silenceSpark()) else None
      synchronized {
        // Find an unused port
        val s = new ServerSocket(0)
        System.setProperty(sparkPortProperty, s.getLocalPort.toString)
        // Allow Spark to take the port we just discovered
        s.close()
        // Create a spark context
        sc = new SparkContext("local[4]", name)
      }
      try {
        // Run the test
        body
      }
      finally {
        // Stop the context
        sc.stop()
        sc = null
        maybeLevels match {
          case None =>
          case Some(levels) =>
            for ((className, level) <- levels) {
              SparkLogUtil.setLogLevels(level, List(className))
            }
        }
      }
    }
  }

}

