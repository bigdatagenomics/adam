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
package org.bdgenomics.adam.cli

import org.scalatest.FunSuite

class AboutSuite extends FunSuite {
  val about = new About()

  test("template variables have been replaced") {
    assert(about.artifactId() !== "${project.artifactId}")
    assert(about.buildTimestamp() !== "${maven.build.timestamp}")
    assert(about.commit() !== "${git.commit.id}")
    assert(about.hadoopVersion() !== "${hadoop.version}")
    assert(about.scalaVersion() !== "${scala.version}")
    assert(about.version() !== "${version}")
  }

  test("templated values are not empty") {
    assert(about.artifactId() !== null)
    assert(!about.artifactId().isEmpty)

    assert(about.buildTimestamp() !== null)
    assert(!about.buildTimestamp().isEmpty)

    assert(about.commit() !== null)
    assert(!about.commit().isEmpty)

    assert(about.hadoopVersion() !== null)
    assert(!about.hadoopVersion().isEmpty)

    assert(about.scalaVersion() !== null)
    assert(!about.scalaVersion().isEmpty)

    assert(about.version() !== null)
    assert(!about.version().isEmpty())
  }
}
