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

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import java.net.ServerSocket
import org.apache.log4j.Level
import edu.berkeley.cs.amplab.adam.serialization.AdamKryoProperties

object SparkTest extends org.scalatest.Tag("edu.berkeley.cs.amplab.util.SparkFunSuite")

trait SparkFunSuite extends FunSuite with BeforeAndAfter {

  val sparkPortProperty = "spark.driver.port"

  var sc: SparkContext = _
  var maybeLevels: Option[Map[String, Level]] = None

  def createSpark(sparkName: String, silenceSpark: Boolean = true): SparkContext = {
    // Use the same context properties as ADAM commands
    AdamKryoProperties.setupContextProperties()
    // Silence the Spark logs if requested
    maybeLevels = if (silenceSpark) Some(SparkLogUtil.silenceSpark()) else None
    synchronized {
      // Find an unused port
      val s = new ServerSocket(0)
      System.setProperty(sparkPortProperty, s.getLocalPort.toString)
      // Allow Spark to take the port we just discovered
      s.close()

      // Create a spark context
      new SparkContext("local[4]", sparkName)
    }
  }

  def destroySpark() {
    // Stop the context
    sc.stop()
    sc = null

    // See notes at:
    // http://blog.quantifind.com/posts/spark-unit-test/
    // That post calls for clearing 'spark.master.port', but this thread
    // https://groups.google.com/forum/#!topic/spark-users/MeVzgoJXm8I
    // suggests that the property was renamed 'spark.driver.port'
    System.clearProperty(sparkPortProperty)

    maybeLevels match {
      case None =>
      case Some(levels) =>
        for ((className, level) <- levels) {
          SparkLogUtil.setLogLevels(level, List(className))
        }
    }
  }

  def sparkBefore(beforeName: String, silenceSpark: Boolean = true)(body: => Unit) {
    before {
      sc = createSpark(beforeName, silenceSpark)
      try {
        // Run the before block
        body
      }
      finally {
        destroySpark()
      }
    }
  }

  def sparkAfter(beforeName: String, silenceSpark: Boolean = true)(body: => Unit) {
    after {
      sc = createSpark(beforeName, silenceSpark)
      try {
        // Run the after block
        body
      }
      finally {
        destroySpark()
      }
    }
  }

  def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
    test(name, SparkTest) {
      sc = createSpark(name, silenceSpark)
      try {
        // Run the test
        body
      }
      finally {
        destroySpark()
      }
    }
  }

}

