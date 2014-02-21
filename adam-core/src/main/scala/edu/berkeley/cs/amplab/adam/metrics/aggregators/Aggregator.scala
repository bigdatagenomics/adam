/*
 * Copyright 2014 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.metrics.aggregators

import java.io.Writer
import edu.berkeley.cs.amplab.adam.util.Histogram
import edu.berkeley.cs.amplab.adam.metrics

trait Aggregator[SingleType, AggType <: Aggregated[SingleType]] extends Serializable {

  /**
   * @return An initial value for the aggregation
   */
  def initialValue: AggType

  /**
   * Aggregates a single sequence of values (which are produced by a single record-pair)
   *
   * @param value The Seq[SingleType] produced by a Comparisons invocation on a ReadBucket
   * @return The aggregation of that sequence.
   */
  def lift(value : Seq[SingleType]) : AggType

  /**
   * Aggregation function to combine the result of a computation with prior results.
   *
   * @param first The first aggregation
   * @param second The second aggregation
   * @return An aggregation combining the values in the first and second aggregations.
   */
  def combine(first: AggType, second: AggType): AggType

}


trait Writable {
  def write(stream : Writer)
}

trait Aggregated[+T] extends Writable with Serializable {
  def count() : Long
  def countIdentical() : Long
}

class AggregatedCollection[T,U <: Aggregated[T]](val values : Seq[U])
  extends Aggregated[metrics.Collection[Seq[T]]]
  with Serializable {

  def count(): Long = values.map( _.count() ).reduce( _ + _ )
  def countIdentical(): Long =
    values.map( _.countIdentical() ).reduce( _ + _ )

  def write(stream: Writer) {
    values.foreach( value => value.write(stream) )
  }
}

object AggregatedCollection {
  def apply[T, U <: Aggregated[T]](values : Seq[U]) = new AggregatedCollection[T,U](values)
}

class CombinedAggregator[Single, Agg <: Aggregated[Single]](aggs : Seq[Aggregator[Single,Agg]])
  extends Aggregator[metrics.Collection[Seq[Single]], AggregatedCollection[Single,Agg]] {

  def initialValue: AggregatedCollection[Single, Agg] = AggregatedCollection(aggs.map(_.initialValue))

  def liftCollection(c : metrics.Collection[Seq[Single]]) : AggregatedCollection[Single,Agg] =
    AggregatedCollection(aggs.zip(c.values).map {
      case (agg : Aggregator[Single,Agg], values : Seq[Single]) =>
        agg.lift(values)
    })

  def lift(values: Seq[metrics.Collection[Seq[Single]]]): AggregatedCollection[Single, Agg] =
    values.map(liftCollection).reduce(combine)

  /**
   * Aggregation function to combine the result of a computation with prior results.
   */
  def combine(first: AggregatedCollection[Single, Agg], second: AggregatedCollection[Single, Agg]): AggregatedCollection[Single, Agg] =
    AggregatedCollection(aggs.zip(first.values.zip(second.values)).map {
      case (agg : Aggregator[Single,Agg], p : (Agg, Agg)) =>
        agg.combine(p._1, p._2)
    })
}

class UniqueAggregator[T] extends Aggregator[T,UniqueWritable[T]] {
  /**
   * An initial value for the aggregation
   */
  def initialValue: UniqueWritable[T] = new UniqueWritable[T]()

  def lift(value: Seq[T]): UniqueWritable[T] = new UniqueWritable[T](value : _*)

  /**
   * Aggregation function to combine the result of a computation with prior results.
   */
  def combine(first: UniqueWritable[T], second: UniqueWritable[T]): UniqueWritable[T] = first.union(second)
}

class HistogramAggregator[T] extends Aggregator[T,Histogram[T]] {
  /**
   * An initial value for the aggregation
   */
  def initialValue: Histogram[T] = Histogram()

  def lift(value: Seq[T]): Histogram[T] = Histogram(value)

  /**
   * Aggregation function to combine the result of a computation with prior results.
   */
  def combine(first: Histogram[T], second: Histogram[T]): Histogram[T] = first ++ second

}


class UniqueWritable[T]( vals : T* ) extends Aggregated[T] with Serializable {

  val count = vals.size.toLong
  val countIdentical = 0L

  val values = vals.toSet

  def union(other : UniqueWritable[T]) : UniqueWritable[T] =
    new UniqueWritable[T](values.union(other.values).toSeq : _*)

  def write(stream : Writer) {
    stream.append("values\n")
    for(value <- values) {
      stream.append("%s\n".format(value.toString))
    }
  }
}
