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
package org.bdgenomics.adam.io

import java.io.{ File, PrintWriter }
import java.net.{ ServerSocket, URI }
import java.util.concurrent.atomic.AtomicInteger

import com.amazonaws.services.s3.AmazonS3Client
import org.bdgenomics.adam.util.{ CredentialsProperties, NetworkConnected, S3Test }
import org.scalatest.FunSuite

class ByteAccessSuite extends FunSuite {

  lazy val credentials = new CredentialsProperties(Some(new File(System.getProperty("user.home") + "/spark.conf")))
    .awsCredentials(Some("s3"))

  lazy val bucketName = System.getenv("BUCKET_NAME")
  lazy val parquetLocation = System.getenv("PARQUET_LOCATION")

  test("ByteArrayByteAccess returns arbitrary subsets of bytes correctly") {
    val bytes = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val access = new ByteArrayByteAccess(bytes)

    assert(access.length() === bytes.length)
    assert(access.readFully(5, 5) === bytes.slice(5, 10))
  }

  test("ByteArrayByteAccess supports two successive calls with different offsets") {
    val bytes = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val access = new ByteArrayByteAccess(bytes)

    assert(access.length() === bytes.length)
    assert(access.readFully(5, 5) === bytes.slice(5, 10))
    assert(access.readFully(3, 5) === bytes.slice(3, 8))
  }

  test("LocalFileByteAccess returns arbitrary subsets of bytes correctly") {
    val content = "abcdefghij"
    val temp = File.createTempFile("byteaccesssuite", "test")

    val writer = new PrintWriter(temp)
    writer.print(content)
    writer.close()

    val access = new LocalFileByteAccess(temp)
    assert(access.length() === content.length())
    assert(access.readFully(3, 5) === content.substring(3, 8).getBytes("ASCII"))
  }

  test("HTTPRangedByteAccess will retry multiple times", NetworkConnected) {
    val socket = new ServerSocket(0)
    if (!socket.isBound) throw new Exception("Could not bind ServerSocket")
    val port = socket.getLocalPort
    val count = new AtomicInteger(0)
    val thread = new Thread(new Runnable {
      def single() = {
        val client = socket.accept()
        count.getAndIncrement
        client.close()
      }

      override def run(): Unit = {
        single()
        run()
      }
    })

    thread.start()

    val uri = URI.create("http://localhost:" + port + "/")
    val http = new HTTPRangedByteAccess(uri, 9)
    intercept[Exception] {
      http.length()
    }
    assert(count.get() === 10, "Retrying 9 times should result in 10 connections")
    socket.close()
    thread.interrupt()
  }

  test("HTTPRangedByteAccess supports range queries", NetworkConnected) {
    val uri = URI.create("https://s3.amazonaws.com/bdgenomics-test/mouse_chrM.bam")
    val http = new HTTPRangedByteAccess(uri, 1)
    val bytes1 = http.readFully(100, 10)
    val bytes2 = http.readFully(100, 100)

    assert(bytes1.length === 10)
    assert(bytes2.length === 100)
    assert(bytes1 === bytes2.slice(0, 10))

    // figured this out by executing:
    // curl --range 100-109 http://www.cs.berkeley.edu/~massie/bams/mouse_chrM.bam | od -t u1
    assert(bytes1 === Array(188, 185, 119, 110, 102, 222, 76, 23, 189, 139).map(_.toByte))
  }

  test("HTTPRangedByteAccess can retrieve a full range", NetworkConnected) {
    val uri = URI.create("https://s3.amazonaws.com/bdgenomics-test/eecslogo.gif")
    val http = new HTTPRangedByteAccess(uri, 1)
    val bytes = http.readFully(0, http.length().toInt)
    assert(bytes.length === http.length())
  }

  test("Testing S3 byte access", NetworkConnected, S3Test) {
    val byteAccess = new S3ByteAccess(new AmazonS3Client(credentials),
      bucketName,
      parquetLocation)
    assert(byteAccess.readFully(0, 1)(0) === 80)
  }
}
