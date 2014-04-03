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

package org.bdgenomics.adam.util

import org.apache.log4j.{Level, Logger}

object SparkLogUtil {

  /**
   * set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level Log4j level
   * @param loggers Loggers to apply level to
   * @return
   */
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map {
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

  /**
   * turn off most of spark logging.  Returns a map of the previous values so you can turn logging back to its
   * former values
   */
  def silenceSpark() = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

}
