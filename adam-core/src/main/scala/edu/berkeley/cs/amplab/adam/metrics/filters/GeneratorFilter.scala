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
package edu.berkeley.cs.amplab.adam.metrics.filters

import edu.berkeley.cs.amplab.adam.metrics.{CombinedComparisons, BucketComparisons}
import edu.berkeley.cs.amplab.adam.metrics

trait GeneratorFilter[+T] extends Serializable {
  def passesFilter(value : Any) : Boolean
  def generator : BucketComparisons[T]
}

abstract class ComparisonsFilter[+T](val generator : BucketComparisons[T]) extends GeneratorFilter[T] {}

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

  def generator: BucketComparisons[metrics.Collection[Seq[T]]] = new CombinedComparisons[T](filters.map(_.generator))
}
