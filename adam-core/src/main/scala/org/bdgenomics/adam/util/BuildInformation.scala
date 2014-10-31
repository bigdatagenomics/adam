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
package org.bdgenomics.adam.core.util

import java.util.Properties

import scala.collection.JavaConverters._

/**
 * A static object that gets the resource "git.properties" and renders the
 * build-information in different formats. The parsing is "lazy", it parses
 * the properties only the first time a function is called.
 */
object BuildInformation {

  private var properties: scala.Option[scala.collection.mutable.Map[String, String]] = None

  /** Return a scala mutable Map (property, value).  */
  def asMap(): scala.collection.mutable.Map[String, String] = {
    properties match {
      case Some(inThere) => inThere
      case None => {
        val javaProperties = new Properties;
        javaProperties.load(getClass().getClassLoader().getResourceAsStream("git.properties"));
        properties = Some(javaProperties.asScala);
        javaProperties.asScala
      }
    }
  }

  /** Return a formatted human-readable string. */
  def asString(): String = {
    asMap().foldLeft("") {
      case (prev_string, (key, value)) =>
        prev_string + key + ": " + value + "\n"
    }
  }
}
