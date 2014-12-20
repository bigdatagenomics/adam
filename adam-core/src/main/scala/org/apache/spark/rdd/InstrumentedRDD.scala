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
package org.apache.spark.rdd

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.annotation.{ DeveloperApi, Experimental }
import org.apache.spark.partial.{ BoundedDouble, PartialResult }
import org.apache.spark.rdd.InstrumentedRDD._
import org.apache.spark.util.Utils
import org.apache.spark.{ Partition, Partitioner, TaskContext }
import org.bdgenomics.adam.instrumentation.{ Clock, Metrics, MetricsRecorder }
import scala.collection.Map
import scala.reflect.ClassTag

/**
 * An RDD which instruments its operations. For further details and usage instructions see the [[MetricsContext]] class.
 *
 * @note This class needs to be in the org.apache.spark.rdd package, otherwise Spark will record the incorrect
 *       call site (which in turn becomes the stage name). This can be fixed when we use Spark 1.1.1 (needs SPARK-1853).
 */
class InstrumentedRDD[T: ClassTag](private[rdd] val decoratedRDD: RDD[T])
    extends RDD[T](decoratedRDD.sparkContext, decoratedRDD.dependencies) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = decoratedRDD.compute(split, context)

  override protected def getPartitions: Array[Partition] = decoratedRDD.partitions

  // Instrumented RDD Operations

  override def map[U](f: (T) => U)(implicit evidence$3: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.map((t) => recordFunction(f(t))))
  }

  override def flatMap[U](f: (T) => TraversableOnce[U])(implicit evidence$4: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.flatMap((t) => recordFunction(f(t))))
  }

  override def filter(f: (T) => Boolean): RDD[T] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.filter((t) => recordFunction(f(t))))
  }

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.distinct(numPartitions))
  }

  override def distinct(): RDD[T] = recordOperation {
    instrument(decoratedRDD.distinct())
  }

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.repartition(numPartitions))
  }

  override def coalesce(numPartitions: Int, shuffle: Boolean)(implicit ord: Ordering[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.coalesce(numPartitions, shuffle))
  }

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] = recordOperation {
    instrument(decoratedRDD.sample(withReplacement, fraction, seed))
  }

  override def randomSplit(weights: Array[Double], seed: Long): Array[RDD[T]] = recordOperation {
    decoratedRDD.randomSplit(weights, seed).map(instrument(_))
  }

  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[T] = recordOperation {
    decoratedRDD.takeSample(withReplacement, num, seed)
  }

  override def union(other: RDD[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.union(other))
  }

  override def ++(other: RDD[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.++(other))
  }

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.sortBy((t) => recordFunction(f(t)), ascending, numPartitions))
  }

  override def intersection(other: RDD[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.intersection(other))
  }

  override def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.intersection(other, partitioner))
  }

  override def intersection(other: RDD[T], numPartitions: Int): RDD[T] = recordOperation {
    instrument(decoratedRDD.intersection(other, numPartitions))
  }

  override def glom(): RDD[Array[T]] = recordOperation {
    instrument(decoratedRDD.glom())
  }

  override def cartesian[U](other: RDD[U])(implicit evidence$5: ClassTag[U]): RDD[(T, U)] = recordOperation {
    instrument(decoratedRDD.cartesian(other))
  }

  override def groupBy[K](f: (T) => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.groupBy((t) => recordFunction(f(t))))
  }

  override def groupBy[K](f: (T) => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.groupBy((t: T) => recordFunction(f(t)), numPartitions))
  }

  override def groupBy[K](f: (T) => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K]): RDD[(K, Iterable[T])] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.groupBy((t: T) => recordFunction(f(t)), p))
  }

  override def pipe(command: String): RDD[String] = recordOperation {
    instrument(decoratedRDD.pipe(command))
  }

  override def pipe(command: String, env: Map[String, String]): RDD[String] = recordOperation {
    instrument(decoratedRDD.pipe(command, env))
  }

  override def pipe(command: Seq[String], env: Map[String, String], printPipeContext: ((String) => Unit) => Unit, printRDDElement: (T, (String) => Unit) => Unit, separateWorkingDir: Boolean): RDD[String] = recordOperation {
    instrument(decoratedRDD.pipe(command, env, printPipeContext, printRDDElement, separateWorkingDir))
  }

  override def mapPartitions[U](f: (Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$6: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.mapPartitions((it) => recordFunction(f(it)), preservesPartitioning))
  }

  override def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$7: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.mapPartitionsWithIndex((i, it) => recordFunction(f(i, it)), preservesPartitioning))
  }

  @DeveloperApi
  override def mapPartitionsWithContext[U](f: (TaskContext, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$8: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.mapPartitionsWithContext((t, it) => recordFunction(f(t, it)), preservesPartitioning))
  }

  @deprecated("use mapPartitionsWithIndex")
  override def mapPartitionsWithSplit[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$9: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.mapPartitionsWithSplit((i, it) => recordFunction(f(i, it)), preservesPartitioning))
  }

  @deprecated("use mapPartitionsWithIndex")
  override def mapWith[A, U](constructA: (Int) => A, preservesPartitioning: Boolean)(f: (T, A) => U)(implicit evidence$10: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.mapWith((i: Int) => recordFunction(constructA(i)), preservesPartitioning)((t, a) => recordFunction(f(t, a))))
  }

  @deprecated("use mapPartitionsWithIndex and flatMap")
  override def flatMapWith[A, U](constructA: (Int) => A, preservesPartitioning: Boolean)(f: (T, A) => Seq[U])(implicit evidence$11: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.flatMapWith((i: Int) => recordFunction(constructA(i)), preservesPartitioning)((t, a) => recordFunction(f(t, a))))
  }

  @deprecated("use mapPartitionsWithIndex and foreach")
  override def foreachWith[A](constructA: (Int) => A)(f: (T, A) => Unit): Unit = recordOperation {
    implicit val recorder = functionRecorder()
    decoratedRDD.foreachWith((i: Int) => recordFunction(constructA(i)))((t, a) => recordFunction(f(t, a)))
  }

  @deprecated("use mapPartitionsWithIndex and filter")
  override def filterWith[A](constructA: (Int) => A)(p: (T, A) => Boolean): RDD[T] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.filterWith((i: Int) => recordFunction(constructA(i)))((t, a) => recordFunction(p(t, a))))
  }

  override def zip[U](other: RDD[U])(implicit evidence$12: ClassTag[U]): RDD[(T, U)] = recordOperation {
    instrument(decoratedRDD.zip(other))
  }

  override def zipPartitions[B, V](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$13: ClassTag[B], evidence$14: ClassTag[V]): RDD[V] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.zipPartitions(rdd2, preservesPartitioning)((it1, it2) => recordFunction(f(it1, it2))))
  }

  override def zipPartitions[B, V](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$15: ClassTag[B], evidence$16: ClassTag[V]): RDD[V] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.zipPartitions(rdd2)((it1, it2) => recordFunction(f(it1, it2))))
  }

  override def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$17: ClassTag[B], evidence$18: ClassTag[C], evidence$19: ClassTag[V]): RDD[V] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.zipPartitions(rdd2, rdd3, preservesPartitioning)((it1, it2, it3) => recordFunction(f(it1, it2, it3))))
  }

  override def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$20: ClassTag[B], evidence$21: ClassTag[C], evidence$22: ClassTag[V]): RDD[V] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.zipPartitions(rdd2, rdd3)((it1, it2, it3) => recordFunction(f(it1, it2, it3))))
  }

  override def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$23: ClassTag[B], evidence$24: ClassTag[C], evidence$25: ClassTag[D], evidence$26: ClassTag[V]): RDD[V] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning)((it1, it2, it3, it4) => recordFunction(f(it1, it2, it3, it4))))
  }

  override def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$27: ClassTag[B], evidence$28: ClassTag[C], evidence$29: ClassTag[D], evidence$30: ClassTag[V]): RDD[V] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.zipPartitions(rdd2, rdd3, rdd4)((it1, it2, it3, it4) => recordFunction(f(it1, it2, it3, it4))))
  }

  override def foreach(f: (T) => Unit): Unit = recordOperation {
    implicit val recorder = functionRecorder()
    decoratedRDD.foreach((t) => recordFunction(f(t)))
  }

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = recordOperation {
    implicit val recorder = functionRecorder()
    decoratedRDD.foreachPartition((it) => recordFunction(f(it)))
  }

  override def collect(): Array[T] = recordOperation {
    decoratedRDD.collect()
  }

  override def toLocalIterator: Iterator[T] = recordOperation {
    decoratedRDD.toLocalIterator
  }

  @deprecated("use collect")
  override def toArray(): Array[T] = recordOperation {
    decoratedRDD.toArray()
  }

  override def collect[U](f: PartialFunction[T, U])(implicit evidence$31: ClassTag[U]): RDD[U] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.collect(recordFunction(f)))
  }

  override def subtract(other: RDD[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.subtract(other))
  }

  override def subtract(other: RDD[T], numPartitions: Int): RDD[T] = recordOperation {
    instrument(decoratedRDD.subtract(other, numPartitions))
  }

  override def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T]): RDD[T] = recordOperation {
    instrument(decoratedRDD.subtract(other, p))
  }

  override def reduce(f: (T, T) => T): T = recordOperation {
    implicit val recorder = functionRecorder()
    decoratedRDD.reduce((t, t2) => recordFunction(f(t, t2)))
  }

  override def fold(zeroValue: T)(op: (T, T) => T): T = recordOperation {
    implicit val recorder = functionRecorder()
    decoratedRDD.fold(zeroValue)((t, t2) => recordFunction(op(t, t2)))
  }

  override def aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit evidence$32: ClassTag[U]): U = recordOperation {
    implicit val recorder = functionRecorder()
    decoratedRDD.aggregate(zeroValue)((u, t) => recordFunction(seqOp(u, t)), recordFunction((u, u2) => combOp(u, u2)))
  }

  override def count(): Long = recordOperation {
    decoratedRDD.count()
  }

  @Experimental
  override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] = recordOperation {
    decoratedRDD.countApprox(timeout, confidence)
  }

  override def countByValue()(implicit ord: Ordering[T]): Map[T, Long] = recordOperation {
    decoratedRDD.countByValue()
  }

  @Experimental
  override def countByValueApprox(timeout: Long, confidence: Double)(implicit ord: Ordering[T]): PartialResult[Map[T, BoundedDouble]] = recordOperation {
    decoratedRDD.countByValueApprox(timeout, confidence)
  }

  @Experimental
  override def countApproxDistinct(p: Int, sp: Int): Long = recordOperation {
    decoratedRDD.countApproxDistinct(p, sp)
  }

  override def countApproxDistinct(relativeSD: Double): Long = recordOperation {
    decoratedRDD.countApproxDistinct(relativeSD)
  }

  override def zipWithIndex(): RDD[(T, Long)] = recordOperation {
    instrument(decoratedRDD.zipWithIndex())
  }

  override def zipWithUniqueId(): RDD[(T, Long)] = recordOperation {
    instrument(decoratedRDD.zipWithUniqueId())
  }

  override def take(num: Int): Array[T] = recordOperation {
    decoratedRDD.take(num)
  }

  override def first(): T = recordOperation {
    decoratedRDD.first()
  }

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] = recordOperation {
    decoratedRDD.top(num)
  }

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = recordOperation {
    decoratedRDD.takeOrdered(num)
  }

  override def max()(implicit ord: Ordering[T]): T = recordOperation {
    decoratedRDD.max()
  }

  override def min()(implicit ord: Ordering[T]): T = recordOperation {
    decoratedRDD.min()
  }

  override def saveAsTextFile(path: String): Unit = recordOperation {
    decoratedRDD.saveAsTextFile(path)
  }

  override def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = recordOperation {
    decoratedRDD.saveAsTextFile(path, codec)
  }

  override def saveAsObjectFile(path: String): Unit = recordOperation {
    decoratedRDD.saveAsObjectFile(path)
  }

  override def keyBy[K](f: (T) => K): RDD[(K, T)] = recordOperation {
    implicit val recorder = functionRecorder()
    instrument(decoratedRDD.keyBy((t) => recordFunction(f(t))))
  }

}

object InstrumentedRDD {

  var clock = new Clock()

  var functionTimer = new Timer("function call")

  /**
   * Instruments the passed-in RDD, returning an [[InstrumentedRDD]]
   */
  def instrument[B: ClassTag](rdd: RDD[B]): RDD[B] = new InstrumentedRDD[B](rdd)

  /**
   * Instruments an RDD operation. All RDD operations that need to be instrumented should be wrapped in this method.
   * See the class-level documentation for a usage example.
   */
  def recordOperation[A](operation: => A): A = {
    rddOperationTimer().time {
      operation
    }
  }

  /**
   * Obtains a metrics recorder suitable for passing-in to a function that operates on an RDD.
   * See the class-level documentation for a usage example.
   */
  def functionRecorder(): FunctionRecorder = {
    val existingRegistryOption = Metrics.Recorder.value
    // Make a copy of the existing registry, as otherwise the stack will be unwound without having measured
    // the timings within the RDD operation
    val metricsRecorder = if (existingRegistryOption.isDefined) Some(existingRegistryOption.get.copy()) else None
    new FunctionRecorder(metricsRecorder, functionTimer)
  }

  /**
   * Instruments a function call that acts on the data in an RDD. The recorder should have been obtained
   * from the `metricsRecorder` method. The passed-in `functionTimer` is used to time the function call.
   * See the class-level documentation for a usage example.
   *
   * The overhead of instrumenting a function call has been measured at around 120 nanoseconds on an Intel i7-3720QM.
   * The overhead of calling this method when no metrics are being recorded (a recorder is not defined) is negligible.
   */
  def recordFunction[B](function: => B)(implicit functionRecorder: FunctionRecorder): B = {
    val recorder = functionRecorder.recorder
    val functionTimer = functionRecorder.functionTimer
    if (recorder.isDefined) {
      Metrics.Recorder.withValue(recorder) {
        functionTimer.time(function)
      }
    } else {
      function
    }
  }

  private def rddOperationTimer(): Timer = {
    // We can only do this because we are in an org.apache.spark package (Utils is private to Spark). When we fix that
    // we'll have to implement our own getCallSite function
    val callSite = Utils.getCallSite.shortForm
    new Timer(callSite, clock = clock, recorder = None,
      sequenceId = Some(Metrics.generateNewSequenceId()), isRDDOperation = true)
  }

}

class FunctionRecorder(val recorder: Option[MetricsRecorder], val functionTimer: Timer) extends Serializable