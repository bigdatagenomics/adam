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
package org.bdgenomics.adam.metrics.filters

import org.bdgenomics.adam.metrics.{CombinedComparisons, BucketComparisons}
import org.bdgenomics.adam.metrics

/**
 * Used by FindReads, a GeneratorFilter is a predicate on values, which also wraps a particular
 * BucketComparisons object (the thing that produces those values).
 *
 * BucketComparisons will generate a Seq[T] for each ReadBucket it's given -- FindReads then filters
 * the ReadBuckets based on whether <i>any</i> element of the Seq[T] passes the GeneratorFilter (this
 * allows us to handle both read-level and base-level comparison metrics and filters, e.g. 'find all
 * matched ReadBuckets for which <i>some</i> base quality score doesn't match').
 *
 * @tparam T
 */
trait GeneratorFilter[+T] extends Serializable {
  def passesFilter(value : Any) : Boolean
  def comparison : BucketComparisons[T]
}

/**
 * A utility class, so we only have to extend the 'passesFilter' method in subclasses.
 *
 * @param comparison The BucketComparisons value to wrap.
 * @tparam T The type of value produced by the 'generator' argument, and filtered by this class.
 */
abstract class ComparisonsFilter[+T](val comparison : BucketComparisons[T]) extends GeneratorFilter[T] {}

/**
 * CombinedFilter lifts a Sequence of GeneratorFilter[T] filters into a single GeneratorFilter (which filters
 * a vector, here reified as a metrics.Collection value, of Seq[T]).
 * @param filters
 * @tparam T
 */
class CombinedFilter[T](val filters : Seq[GeneratorFilter[T]]) extends GeneratorFilter[metrics.Collection[Seq[T]]] {

  def passesFilter(value: Any): Boolean = {
    value match {
      case valueCollection : metrics.Collection[Seq[T]] =>
        filters.zip(valueCollection.values).forall {
          case (f : GeneratorFilter[T], values : Seq[T]) =>
            values.exists(f.passesFilter(_))
        }
    }
  }

  def comparison: BucketComparisons[metrics.Collection[Seq[T]]] = new CombinedComparisons[T](filters.map(_.comparison))
}
