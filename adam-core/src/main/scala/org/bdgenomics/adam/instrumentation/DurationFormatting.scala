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

import java.text.DecimalFormat
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

/**
 * Functions for formatting durations. The following rules are applied:
 * - Durations greater than one hour are formatted as "X hours X mins X secs"
 * - Durations greater than one minute, but less than one hour are formatted as "X mins X secs"
 * - Durations less than one minute are formatted with two digits after the decimal point. Zeros are suppressed.
 */
object DurationFormatting {

  private val TwoDigitNumberFormatter = new DecimalFormat(".0#")

  /**
   * Formats the passed-in value as a duration. Non-integer values will be rounded
   * before they are formatted.
   */
  def formatMillisecondDuration(number: Number): String = {
    val conversion: (Long) => Long = TimeUnit.MILLISECONDS.toNanos
    formatNumber(number, conversion)
  }

  /**
   * Formats the passed-in value as a duration. Non-integer values will be rounded
   * before they are formatted.
   */
  def formatNanosecondDuration(number: Number): String = {
    val conversion: (Long) => Long = TimeUnit.NANOSECONDS.toNanos
    formatNumber(number, conversion)
  }

  def formatDuration(duration: Duration): String = {

    var nanoDuration = duration.toNanos

    if (nanoDuration < 0) {
      throw new IllegalArgumentException("Duration must be greater or equal to zero!")
    }

    val hours = TimeUnit.NANOSECONDS.toHours(nanoDuration)
    nanoDuration -= TimeUnit.HOURS.toNanos(hours)

    val minutes = TimeUnit.NANOSECONDS.toMinutes(nanoDuration)
    nanoDuration -= TimeUnit.MINUTES.toNanos(minutes)

    val seconds = TimeUnit.NANOSECONDS.toSeconds(nanoDuration)
    nanoDuration -= TimeUnit.SECONDS.toNanos(seconds)

    val millis = TimeUnit.NANOSECONDS.toMillis(nanoDuration)
    nanoDuration -= TimeUnit.MILLISECONDS.toNanos(millis)

    val micros = TimeUnit.NANOSECONDS.toMicros(nanoDuration)
    nanoDuration -= TimeUnit.MICROSECONDS.toNanos(micros)

    val nanos = nanoDuration

    val builder = new StringBuilder()
    if (hours > 0) {
      builder.append(hours)
      builder.append(" hour").append(if (hours != 1) "s" else "")
    }
    if (minutes > 0 || hours > 0) {
      if (hours > 0) {
        builder.append(" ")
      }
      builder.append(minutes)
      builder.append(" min").append(if (minutes != 1) "s" else "")
      builder.append(" ")
      builder.append(seconds)
      builder.append(" sec").append(if (seconds != 1) "s" else "")
    } else {
      if (seconds > 0) {
        formatValue(seconds, millis, builder)
        builder.append(" secs")
      } else {
        if (millis > 0) {
          formatValue(millis, micros, builder)
          builder.append(" ms")
        } else {
          if (micros > 0) {
            formatValue(micros, nanos, builder)
            builder.append(" µs")
          } else if (nanos > 0) {
            builder.append(nanos)
            builder.append(" ns")
          } else {
            builder.append("0")
          }
        }
      }
    }

    builder.toString()

  }

  private def formatValue(largeValue: Long, smallValue: Long, builder: StringBuilder) {
    val totalValue = largeValue + (smallValue / 1000d)
    val stringValue = TwoDigitNumberFormatter.format(totalValue)
    // DecimalFormat doesn't seem to be able to do this for us, even if we specify
    // # as the first digit after the decimal place
    if (stringValue.endsWith(".0")) {
      builder.append(stringValue.substring(0, stringValue.length - ".0".length))
    } else {
      builder.append(stringValue)
    }
  }

  private def formatNumber(number: Number, conversionFunction: (Long) => Long): String = {
    formatDuration(Duration.fromNanos(conversionFunction.apply(number.longValue())))
  }

}
