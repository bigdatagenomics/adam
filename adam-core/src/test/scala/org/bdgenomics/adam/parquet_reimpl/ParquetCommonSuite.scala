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
package org.bdgenomics.adam.parquet_reimpl

import java.net.URI

import com.amazonaws.services.s3.AmazonS3Client
import java.io.File
import org.bdgenomics.adam.rdd.ParquetCommon
import org.bdgenomics.adam.io.{ HTTPRangedByteAccess, LocalFileByteAccess, S3ByteAccess }
import org.bdgenomics.adam.util.{ S3Test, NetworkConnected, CredentialsProperties }
import org.scalatest.FunSuite

class ParquetCommonSuite extends FunSuite {
  lazy val credentials = new CredentialsProperties(new File(System.getProperty("user.home") + "/spark.conf"))
    .awsCredentials(Some("s3"))

  lazy val bucketName = System.getenv("bucket-name")
  lazy val parquetLocation = System.getenv("parquet-location")

  val filename = Thread.currentThread().getContextClassLoader.getResource("small_adam.fgenotype").getFile
  val s3Filename = ""

  test("Can load footer from small vcf") {
    val access = new LocalFileByteAccess(new File(filename))
    val footer = ParquetCommon.readFooter(access)

    assert(footer.rowGroups.length === 1)
  }

  test("Reading a footer from HTTP", NetworkConnected) {
    val byteAccess = new HTTPRangedByteAccess(URI.create("http://www.cs.berkeley.edu/~massie/adams/part0"))
    val footer = ParquetCommon.readFooter(byteAccess)
    assert(footer.rowGroups.length === 1)
  }

  test("Reading a footer from S3", NetworkConnected, S3Test) {
    val byteAccess = new S3ByteAccess(new AmazonS3Client(credentials),
      bucketName,
      parquetLocation)
    val footer = ParquetCommon.readFooter(byteAccess)
    assert(footer.rowGroups.length === 1)
  }
}
