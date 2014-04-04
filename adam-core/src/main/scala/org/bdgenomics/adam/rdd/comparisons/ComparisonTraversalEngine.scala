/*
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.comparisons

import org.bdgenomics.adam.models.ReadBucket
import org.bdgenomics.adam.projections.{ FieldValue, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.Some
import org.apache.hadoop.fs.Path
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.metrics.BucketComparisons
import org.bdgenomics.adam.metrics.aggregators.{ Aggregated, Aggregator }
import org.bdgenomics.adam.metrics.filters.GeneratorFilter

class ComparisonTraversalEngine(schema: Seq[FieldValue], input1: RDD[ADAMRecord], input2: RDD[ADAMRecord])(implicit sc: SparkContext) {
  def this(schema: Seq[FieldValue], input1Paths: Seq[Path], input2Paths: Seq[Path])(implicit sc: SparkContext) =
    this(schema, sc.loadADAMFromPaths(input1Paths), sc.loadADAMFromPaths(input2Paths))(sc)

  lazy val projection = Projection(schema: _*)

  lazy val named1 = input1.adamSingleReadBuckets()
    .map(ReadBucket.singleReadBucketToReadBucket).keyBy(_.allReads.head.getReadName)
  lazy val named2 = input2.adamSingleReadBuckets()
    .map(ReadBucket.singleReadBucketToReadBucket).keyBy(_.allReads.head.getReadName)

  lazy val joined = named1.join(named2)

  def uniqueToNamed1(): Long = {
    named1.leftOuterJoin(named2).filter {
      case (name, (bucket1, Some(bucket2))) => false
      case (name, (bucket1, None)) => true
    }.count()
  }

  def uniqueToNamed2(): Long = {
    named2.leftOuterJoin(named1).filter {
      case (name, (bucket1, Some(bucket2))) => false
      case (name, (bucket1, None)) => true
    }.count()
  }

  def generate[T](generator: BucketComparisons[T]): RDD[(CharSequence, Seq[T])] =
    ComparisonTraversalEngine.generate[T](joined, generator)

  def find[T](filter: GeneratorFilter[T]): RDD[CharSequence] =
    ComparisonTraversalEngine.find[T](joined, filter)
}

object ComparisonTraversalEngine {

  type JoinedType = RDD[(CharSequence, (ReadBucket, ReadBucket))]
  type GeneratedType[T] = RDD[(CharSequence, Seq[T])]

  def generate[T](joined: JoinedType, generator: BucketComparisons[T]): GeneratedType[T] =
    joined.map {
      case (name, (bucket1, bucket2)) =>
        (name, generator.matchedByName(bucket1, bucket2))
    }

  def find[T](joined: JoinedType, filter: GeneratorFilter[T]): RDD[CharSequence] =
    joined.filter {
      case (name, (bucket1, bucket2)) =>
        filter.comparison.matchedByName(bucket1, bucket2).exists(filter.passesFilter)
    }.map(_._1)

  def combine[T, A <: Aggregated[T]: ClassManifest](generated: GeneratedType[T], aggregator: Aggregator[T, A]): A =
    generated.aggregate[A](aggregator.initialValue)(
      (aggregated, namedValue) => aggregator.combine(aggregated, aggregator.lift(namedValue._2)),
      aggregator.combine)

}
