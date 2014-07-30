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

import java.io.InputStream
import java.net.URI

import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{ HttpGet, HttpHead }
import org.apache.http.impl.client.{ HttpClients, StandardHttpRequestRetryHandler }
import org.apache.spark.Logging

/**
 * HTTPRangedByteAccess supports Ranged GET queries against HTTP resources.
 *
 * @param uri The URL of the resource, which should support ranged queries
 * @param retries Number of times to retry a failed connection before giving up
 */
class HTTPRangedByteAccess(uri: URI, retries: Int = 3) extends ByteAccess with Logging {
  private def getClient: HttpClient = {
    HttpClients.custom()
      .setRetryHandler(new StandardHttpRequestRetryHandler(retries, true))
      .build()
  }

  private def readLength(): Long = {
    val head = new HttpHead(uri)
    val response = getClient.execute(head)
    val headers = response.getAllHeaders

    // The spec (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.5) says that
    // Accept-Ranges can return values of 'bytes', 'none', or any other acceptable range-unit;
    // however, "bytes" is the only range-unit recognized in HTTP/1.1 and that clients may choose
    // to ignore other range-unit values.
    // Therefore, we ignore non-standard range values, issuing only a warning when we see them.
    // The spec says that _no_ Accept-Ranges header is possible and the resource may still
    // respect range queries -- but if the Accept-Ranges header _is_ present, and the value is
    // "none", then no range requests are allowed.  In this case, that means we throw an
    // exception.
    headers.find(_.getName == "Accept-Ranges") match {
      case Some(header) => header.getValue match {
        case "none" => throw new IllegalStateException(
          "Server for \"%s\" doesn't accept range requests (\"Accept-Ranges: none\" header in HEAD request response)"
            .format(uri.toString))
        case "bytes" =>
        case value: String => logWarning(
          "Server returned a header of \"Accept-Ranges: %s\", but the only values that we can handle are \"none\" and \"bytes\"".format(value))
      }
      case None =>
    }

    // This is the Content-Length of the entire resource (as opposed to the Content-Length value
    // on the range request, below -- that's just the length of the portion of the resource that
    // was returned).
    headers.find(_.getName == "Content-Length") match {
      case None => throw new IllegalStateException("Unknown length of content for \"%s\" (No \"Content-Length\" header)"
        .format(uri.toString))
      case Some(header) => header.getValue.toLong
    }
  }

  private lazy val _length = readLength()

  /*
   Regex to read the Content-Range response header, as described in:
   http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.16
    */
  private val byteRangeResponseRegex = "bytes\\s((?:\\*|\\d+-\\d+))/((?:\\*|\\d+))".r

  override def length(): Long = _length

  /**
   * In this initial implementation, we throw an error when we get a partial response from the
   * server whose content is less than the bytes we originally requested.
   *
   * @param offset The offset into the resource at which we want to start reading bytes
   * @param length The number of bytes to be read
   * @return An InputStream from a ranged HTTP GET, which should contain just the requested bytes
   */
  override def readByteStream(offset: Long, length: Int): InputStream = {
    val get = new HttpGet(uri)

    // I believe the 'end' coordinate on the range request is _inclusive_
    get.setHeader("Range", "bytes=%d-%d".format(offset, offset + length - 1))

    val response = getClient.execute(get)

    // We want a 206 response to a ranged request, not your normal 200!
    require(response.getStatusLine.getStatusCode == 206,
      "Range request on \"%s\", expected status code 206 but received %d (%s)"
        .format(uri.toString, response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase))

    // This big nested set of case statements is to make sure that
    // 1. we have a Content-Range in the header,
    // 2. that the value of the Content-Range header matches the regex (see above), and
    // 3. that the regex, if it contains a range field "start-end", that the
    //    a) 'start' value matches the 'offset' argument, and
    //    b) 'end' - 'start' + 1 matches the 'length' argument.

    // First, we check condition (1)
    response.getAllHeaders.find(_.getName == "Content-Range") match {
      case None => throw new IllegalStateException("Ranged GET didn't return a Content-Range header")

      // Now, we check condition (2)
      case Some(header) => byteRangeResponseRegex.findFirstMatchIn(header.getValue) match {
        case None => throw new IllegalStateException("Content-Range header value \"%s\" didn't match the expected format".format(header.getValue))

        // Finally, we check condition (3)
        case Some(m) => m.group(1) match {
          case "*" =>
          case str: String => str.split("-").toSeq match {
            case Seq(start, end) =>
              // Here is the check on (3a)
              if (start.toLong != offset)
                throw new IllegalArgumentException(
                  "Content-Range response start %d (from header \"%s\") doesn't match offset %d"
                    .format(start.toLong, header.getValue, offset))
              // here is the check on (3b)
              if (end.toLong - start.toLong + 1 != length)
                throw new IllegalArgumentException(
                  "Content-Range response length %d (from header \"%s\") doesn't match length %d"
                    .format(end.toLong - start.toLong + 1, header.getValue, length))
          }

        }
      }
    }

    response.getEntity.getContent
  }
}
