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

import org.scalatest.FunSuite
import java.io.File

class CredentialsPropertiesTestSuite extends FunSuite {

  test("Can parse a simple configuration file with CredentialsProperties") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val cp = new CredentialsProperties(file)

    val aws = cp.awsCredentials()
    assert(aws.getAWSAccessKeyId === "accessKey")
    assert(aws.getAWSSecretKey === "secretKey")

    val aws_s3 = cp.awsCredentials(Some("s3"))
    assert(aws_s3.getAWSAccessKeyId === "accessKey_s3")
    assert(aws_s3.getAWSSecretKey === "secretKey_s3")

  }

}

class ConfigurationFileSuite extends FunSuite {

  test("Can read values out of a file") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val config = new ConfigurationFile(file)
    assert(config.properties.contains("accessKey"))
    assert(config.properties.contains("secretKey"))
    assert(config.properties.contains("accessKey_s3"))
    assert(config.properties.contains("secretKey_s3"))
    assert(config.properties("accessKey") === "accessKey")
    assert(config.properties("secretKey") === "secretKey")
    assert(config.properties("accessKey_s3") === "accessKey_s3")
    assert(config.properties("secretKey_s3") === "secretKey_s3")
  }

  test("Reads default values when the file does not exist.") {
    val path = "/foo/bar.conf"
    val file = new File(path)
    val defaultMap = Seq("accessKey" -> "foo", "secretKey" -> "bar").toMap
    val config = new ConfigurationFile(file, Some(defaultMap))
    assert(config.properties.contains("accessKey"))
    assert(config.properties.contains("secretKey"))
    assert(config.properties("accessKey") === "foo")
    assert(config.properties("secretKey") === "bar")
  }

  test("Does not read the default values, when the file does exist.") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val defaultMap = Seq("accessKey" -> "foo", "secretKey" -> "bar").toMap
    val config = new ConfigurationFile(file, Some(defaultMap))
    assert(config.properties.contains("accessKey"))
    assert(config.properties.contains("secretKey"))
    assert(config.properties("accessKey") === "accessKey")
    assert(config.properties("secretKey") === "secretKey")
  }

}

class ConfigurationParserSuite extends FunSuite {
  test("parses a simple file") {
    val path = Thread.currentThread().getContextClassLoader.getResource("test.conf").getFile
    val file = new File(path)
    val map: Map[String, String] = ConfigurationParser(file)

    assert(map.size === 4)

    assert(map.contains("accessKey"))
    assert(map.contains("secretKey"))
    assert(map("accessKey") === "accessKey")
    assert(map("secretKey") === "secretKey")

    assert(map.contains("accessKey_s3"))
    assert(map.contains("secretKey_s3"))
    assert(map("accessKey_s3") === "accessKey_s3")
    assert(map("secretKey_s3") === "secretKey_s3")
  }
}
