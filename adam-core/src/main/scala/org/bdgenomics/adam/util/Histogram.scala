/**
 * Copyright 2013 Genome Bridge LLC
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

import java.io.{ StringWriter, Writer }
import org.bdgenomics.adam.metrics.aggregators.Aggregated

class Histogram[T](val valueToCount: Map[T, Int]) extends Aggregated[T] with Serializable {

  /**
   * Counts the total number of elements that went into the histogram
   * @return
   */
  def count(): Long = valueToCount.values.map(_.toLong).reduce(_ + _)

  def countIdentical(): Long = countSubset(defaultFilter)

  /**
   * Counts the number of values in the Histogram whose keys pass the given predicate function, f.
   * @param f Only those keys k for which f(k) == true will be counted.
   * @return The sum of the values passed by the predicate.
   */
  def countSubset(f: (Any) => Boolean): Long = valueToCount.filter {
    case (k: T, v: Int) => f(k)
  }.values.map(_.toLong).reduce(_ + _)

  private def defaultFilter(x: Any): Boolean = {
    x match {
      case (x1: Any, x2: Any) => x1 == x2
      case i: Int => i == 0
      case l: Long => l == 0L
      case b: Boolean => b
      case _ => false
    }
  }

  def ++(other: Histogram[T]): Histogram[T] = {
    val map = collection.mutable.HashMap[T, Int]()
    valueToCount foreach map.+=
    other.valueToCount foreach (kv => {
      val newValue = map.getOrElse(kv._1, 0) + kv._2
      map(kv._1) = newValue
    })
    new Histogram[T](map.toMap)
  }

  def +(value: T): Histogram[T] = {
    val map =
      if (valueToCount.contains(value))
        (valueToCount - value) + (value -> (valueToCount(value) + 1))
      else
        valueToCount ++ Map(value -> 1)
    new Histogram[T](map)
  }

  def write(stream: Writer) {
    stream.append("value\tcount\n")
    for ((value, count) <- valueToCount) {
      stream.append("%s\t%d\n".format(value.toString, count))
    }
  }

  override def toString: String = {
    val stringWriter: StringWriter = new StringWriter()
    write(stringWriter)
    stringWriter.toString
  }
}

object Histogram {

  def apply[T](valueToCount: Map[T, Int]): Histogram[T] =
    new Histogram[T](valueToCount)

  def apply[T](): Histogram[T] =
    new Histogram[T](Map())

  def apply[T](value: T): Histogram[T] =
    new Histogram[T](Map((value, 1)))

  def apply[T](values: Seq[T]): Histogram[T] = {
    new Histogram(Map(values.map(v => (v, 1)): _*))
  }
}
