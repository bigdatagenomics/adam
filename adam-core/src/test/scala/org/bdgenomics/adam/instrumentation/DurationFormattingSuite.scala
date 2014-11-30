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
package org.bdgenomics.adam.instrumentation

import java.util.concurrent.TimeUnit._
import org.bdgenomics.adam.instrumentation.DurationFormatting.formatDuration
import org.scalatest.FunSuite
import scala.concurrent.duration.Duration

class DurationFormattingSuite extends FunSuite {

  test("Values with hours formatted correctly") {
    assert(format(HOURS.toNanos(1)) === "1 hour 0 mins 0 secs")
    assert(format(HOURS.toNanos(23)) === "23 hours 0 mins 0 secs")
    assert(format(HOURS.toNanos(23) + MINUTES.toNanos(1)) === "23 hours 1 min 0 secs")
    assert(format(HOURS.toNanos(23) + MINUTES.toNanos(24)) === "23 hours 24 mins 0 secs")
    assert(format(HOURS.toNanos(23) + MINUTES.toNanos(24) + SECONDS.toNanos(1)) === "23 hours 24 mins 1 sec")
    assert(format(HOURS.toNanos(23) + MINUTES.toNanos(24) + SECONDS.toNanos(25)) === "23 hours 24 mins 25 secs")
    assert(format(HOURS.toNanos(23) + MINUTES.toNanos(24) + SECONDS.toNanos(25) + MILLISECONDS.toNanos(100)) === "23 hours 24 mins 25 secs")
  }

  test("Values with minutes formatted correctly") {
    assert(format(MINUTES.toNanos(1)) === "1 min 0 secs")
    assert(format(MINUTES.toNanos(24)) === "24 mins 0 secs")
    assert(format(MINUTES.toNanos(24) + SECONDS.toNanos(1)) === "24 mins 1 sec")
    assert(format(MINUTES.toNanos(24) + SECONDS.toNanos(25)) === "24 mins 25 secs")
    assert(format(MINUTES.toNanos(24) + SECONDS.toNanos(25) + MILLISECONDS.toNanos(100)) === "24 mins 25 secs")
  }

  test("Values with seconds formatted correctly") {
    assert(format(SECONDS.toNanos(1)) === "1 secs")
    assert(format(SECONDS.toNanos(25)) === "25 secs")
    assert(format(SECONDS.toNanos(25) + MILLISECONDS.toNanos(999)) === "26 secs")
    assert(format(SECONDS.toNanos(5) + MILLISECONDS.toNanos(999)) === "6 secs")
    assert(format(SECONDS.toNanos(25) + MILLISECONDS.toNanos(989)) === "25.99 secs")
    assert(format(SECONDS.toNanos(5) + MILLISECONDS.toNanos(989)) === "5.99 secs")
    assert(format(SECONDS.toNanos(25) + MILLISECONDS.toNanos(100)) === "25.1 secs")
    assert(format(SECONDS.toNanos(5) + MILLISECONDS.toNanos(100)) === "5.1 secs")
    assert(format(SECONDS.toNanos(25) + MILLISECONDS.toNanos(10)) === "25.01 secs")
    assert(format(SECONDS.toNanos(5) + MILLISECONDS.toNanos(10)) === "5.01 secs")
    assert(format(SECONDS.toNanos(25) + MILLISECONDS.toNanos(1)) === "25 secs")
    assert(format(SECONDS.toNanos(5) + MILLISECONDS.toNanos(1)) === "5 secs")
  }

  test("Values with milliseconds formatted correctly") {
    assert(format(MILLISECONDS.toNanos(1)) === "1 ms")
    assert(format(MILLISECONDS.toNanos(25)) === "25 ms")
    assert(format(MILLISECONDS.toNanos(25) + MICROSECONDS.toNanos(999)) === "26 ms")
    assert(format(MILLISECONDS.toNanos(5) + MICROSECONDS.toNanos(999)) === "6 ms")
    assert(format(MILLISECONDS.toNanos(25) + MICROSECONDS.toNanos(989)) === "25.99 ms")
    assert(format(MILLISECONDS.toNanos(5) + MICROSECONDS.toNanos(989)) === "5.99 ms")
    assert(format(MILLISECONDS.toNanos(25) + MICROSECONDS.toNanos(100)) === "25.1 ms")
    assert(format(MILLISECONDS.toNanos(5) + MICROSECONDS.toNanos(100)) === "5.1 ms")
    assert(format(MILLISECONDS.toNanos(25) + MICROSECONDS.toNanos(10)) === "25.01 ms")
    assert(format(MILLISECONDS.toNanos(5) + MICROSECONDS.toNanos(10)) === "5.01 ms")
    assert(format(MILLISECONDS.toNanos(25) + MICROSECONDS.toNanos(1)) === "25 ms")
    assert(format(MILLISECONDS.toNanos(5) + MICROSECONDS.toNanos(1)) === "5 ms")
  }

  test("Values with microseconds formatted correctly") {
    assert(format(MICROSECONDS.toNanos(1)) === "1 µs")
    assert(format(MICROSECONDS.toNanos(25)) === "25 µs")
    assert(format(MICROSECONDS.toNanos(25) + NANOSECONDS.toNanos(999)) === "26 µs")
    assert(format(MICROSECONDS.toNanos(5) + NANOSECONDS.toNanos(999)) === "6 µs")
    assert(format(MICROSECONDS.toNanos(25) + NANOSECONDS.toNanos(989)) === "25.99 µs")
    assert(format(MICROSECONDS.toNanos(5) + NANOSECONDS.toNanos(989)) === "5.99 µs")
    assert(format(MICROSECONDS.toNanos(25) + NANOSECONDS.toNanos(100)) === "25.1 µs")
    assert(format(MICROSECONDS.toNanos(5) + NANOSECONDS.toNanos(100)) === "5.1 µs")
    assert(format(MICROSECONDS.toNanos(25) + NANOSECONDS.toNanos(10)) === "25.01 µs")
    assert(format(MICROSECONDS.toNanos(5) + NANOSECONDS.toNanos(10)) === "5.01 µs")
    assert(format(MICROSECONDS.toNanos(25) + NANOSECONDS.toNanos(1)) === "25 µs")
    assert(format(MICROSECONDS.toNanos(5) + NANOSECONDS.toNanos(1)) === "5 µs")
  }

  test("Values with nanoseconds formatted correctly") {
    assert(format(NANOSECONDS.toNanos(999)) === "999 ns")
    assert(format(NANOSECONDS.toNanos(989)) === "989 ns")
    assert(format(NANOSECONDS.toNanos(100)) === "100 ns")
    assert(format(NANOSECONDS.toNanos(10)) === "10 ns")
    assert(format(NANOSECONDS.toNanos(1)) === "1 ns")
  }

  def format(durationNanos: Long): String = {
    formatDuration(Duration.fromNanos(durationNanos))
  }

}
