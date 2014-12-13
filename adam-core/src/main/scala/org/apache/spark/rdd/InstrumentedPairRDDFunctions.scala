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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{ JobConf, OutputFormat }
import org.apache.hadoop.mapreduce
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.annotation.Experimental
import org.apache.spark.partial.{ BoundedDouble, PartialResult }
import org.apache.spark.rdd.InstrumentedPairRDDFunctions.setRecorder
import org.apache.spark.rdd.InstrumentedRDD._
import org.apache.spark.rdd.MetricsContext.rddToInstrumentedRDD
import org.apache.spark.serializer.Serializer
import org.bdgenomics.adam.instrumentation.{ Metrics, MetricsRecorder }
import scala.collection.Map
import scala.reflect.ClassTag

/**
 * A version of [[PairRDDFunctions]] which enables instrumentation of its operations. For more details
 * and usage instructions see the [[MetricsContext]] class.
 */
class InstrumentedPairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends Serializable {

  def combineByKey[C](createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner, mapSideCombine: Boolean, serializer: Serializer): RDD[(K, C)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.combineByKey((v) => recordFunction(createCombiner(v)), (c, v) => recordFunction(mergeValue(c, v)), (c, c2) => recordFunction(mergeCombiners(c, c2)), partitioner, mapSideCombine, serializer))
    }
    case _ => self.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer)
  }

  def combineByKey[C](createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.combineByKey((v: V) => recordFunction(createCombiner(v)), (c: C, v: V) => recordFunction(mergeValue(c, v)), (c: C, c2: C) => recordFunction(mergeCombiners(c, c2)), numPartitions))
    }
    case _ => self.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions)
  }

  def aggregateByKey[U](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U, combOp: (U, U) => U)(implicit evidence$1: ClassTag[U]): RDD[(K, U)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.aggregateByKey(zeroValue, partitioner)((u, v) => recordFunction(seqOp(u, v)), (u, u2) => recordFunction(combOp(u, u2))))
    }
    case _ => self.aggregateByKey(zeroValue, partitioner)(seqOp, combOp)
  }

  def aggregateByKey[U](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U, combOp: (U, U) => U)(implicit evidence$2: ClassTag[U]): RDD[(K, U)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.aggregateByKey(zeroValue, numPartitions)((u, v) => recordFunction(seqOp(u, v)), (u, u2) => recordFunction(combOp(u, u2))))
    }
    case _ => self.aggregateByKey(zeroValue, numPartitions)(seqOp, combOp)
  }

  def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)(implicit evidence$3: ClassTag[U]): RDD[(K, U)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.aggregateByKey(zeroValue)((u, v) => recordFunction(seqOp(u, v)), (u, u2) => recordFunction(combOp(u, u2))))
    }
    case _ => self.aggregateByKey(zeroValue)(seqOp, combOp)
  }

  def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.foldByKey(zeroValue, partitioner)((v, v2) => recordFunction(func(v, v2))))
    }
    case _ => self.foldByKey(zeroValue, partitioner)(func)
  }

  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.foldByKey(zeroValue, numPartitions)((v, v2) => recordFunction(func(v, v2))))
    }
    case _ => self.foldByKey(zeroValue, numPartitions)(func)
  }

  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.foldByKey(zeroValue)((v, v2) => recordFunction(func(v, v2))))
    }
    case _ => self.foldByKey(zeroValue)(func)
  }

  def sampleByKey(withReplacement: Boolean, fractions: Map[K, Double], seed: Long): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.sampleByKey(withReplacement, fractions, seed))
    }
    case _ => self.sampleByKey(withReplacement, fractions, seed)
  }

  @Experimental
  def sampleByKeyExact(withReplacement: Boolean, fractions: Map[K, Double], seed: Long): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.sampleByKeyExact(withReplacement, fractions, seed))
    }
    case _ => self.sampleByKeyExact(withReplacement, fractions, seed)
  }

  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.reduceByKey(partitioner, (v, v2) => recordFunction(func(v, v2))))
    }
    case _ => self.reduceByKey(partitioner, func)
  }

  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.reduceByKey((v, v2) => recordFunction(func(v, v2)), numPartitions))
    }
    case _ => self.reduceByKey(func, numPartitions)
  }

  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.reduceByKey((v, v2) => recordFunction(func(v, v2))))
    }
    case _ => self.reduceByKey(func)
  }

  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      self.reduceByKeyLocally((v, v2) => recordFunction(func(v, v2)))
    }
    case _ => self.reduceByKeyLocally(func)
  }

  @deprecated("Use reduceByKeyLocally")
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      self.reduceByKeyToDriver((v, v2) => recordFunction(func(v, v2)))
    }
    case _ => self.reduceByKeyToDriver(func)
  }

  def countByKey(): Map[K, Long] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      self.countByKey()
    }
    case _ => self.countByKey()
  }

  @Experimental
  def countByKeyApprox(timeout: Long, confidence: Double): PartialResult[Map[K, BoundedDouble]] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      self.countByKeyApprox(timeout, confidence)
    }
    case _ => self.countByKeyApprox(timeout, confidence)
  }

  @Experimental
  def countApproxDistinctByKey(p: Int, sp: Int, partitioner: Partitioner): RDD[(K, Long)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.countApproxDistinctByKey(p, sp, partitioner))
    }
    case _ => self.countApproxDistinctByKey(p, sp, partitioner)
  }

  def countApproxDistinctByKey(relativeSD: Double, partitioner: Partitioner): RDD[(K, Long)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.countApproxDistinctByKey(relativeSD, partitioner))
    }
    case _ => self.countApproxDistinctByKey(relativeSD, partitioner)
  }

  def countApproxDistinctByKey(relativeSD: Double, numPartitions: Int): RDD[(K, Long)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.countApproxDistinctByKey(relativeSD, numPartitions))
    }
    case _ => self.countApproxDistinctByKey(relativeSD, numPartitions)
  }

  def countApproxDistinctByKey(relativeSD: Double): RDD[(K, Long)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.countApproxDistinctByKey(relativeSD))
    }
    case _ => self.countApproxDistinctByKey(relativeSD)
  }

  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.groupByKey(partitioner))
    }
    case _ => self.groupByKey(partitioner)
  }

  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.groupByKey(numPartitions))
    }
    case _ => self.groupByKey(numPartitions)
  }

  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.partitionBy(partitioner))
    }
    case _ => self.partitionBy(partitioner)
  }

  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.join(other, partitioner))
    }
    case _ => self.join(other, partitioner)
  }

  def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.leftOuterJoin(other, partitioner))
    }
    case _ => self.leftOuterJoin(other, partitioner)
  }

  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.rightOuterJoin(other, partitioner))
    }
    case _ => self.rightOuterJoin(other, partitioner)
  }

  def combineByKey[C](createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C): RDD[(K, C)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.combineByKey((v) => recordFunction(createCombiner(v)), (c, v) => recordFunction(mergeValue(c, v)), (c, c2) => recordFunction(mergeCombiners(c, c2))))
    }
    case _ => self.combineByKey(createCombiner, mergeValue, mergeCombiners)
  }

  def groupByKey(): RDD[(K, Iterable[V])] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.groupByKey())
    }
    case _ => self.groupByKey()
  }

  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.join(other))
    }
    case _ => self.join(other)
  }

  def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.join(other, numPartitions))
    }
    case _ => self.join(other, numPartitions)
  }

  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.leftOuterJoin(other))
    }
    case _ => self.leftOuterJoin(other)
  }

  def leftOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, Option[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.leftOuterJoin(other, numPartitions))
    }
    case _ => self.leftOuterJoin(other, numPartitions)
  }

  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.rightOuterJoin(other))
    }
    case _ => self.rightOuterJoin(other)
  }

  def rightOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], W))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.rightOuterJoin(other, numPartitions))
    }
    case _ => self.rightOuterJoin(other, numPartitions)
  }

  def collectAsMap(): Map[K, V] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      self.collectAsMap()
    }
    case _ => self.collectAsMap()
  }

  def mapValues[U](f: (V) => U): RDD[(K, U)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.mapValues((v) => recordFunction(f(v))))
    }
    case _ => self.mapValues(f)
  }

  def flatMapValues[U](f: (V) => TraversableOnce[U]): RDD[(K, U)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      implicit val recorder = functionRecorder()
      instrument(self.flatMapValues((v) => recordFunction(f(v))))
    }
    case _ => self.flatMapValues(f)
  }

  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other1, other2, other3, partitioner))
    }
    case _ => self.cogroup(other1, other2, other3, partitioner)
  }

  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other, partitioner))
    }
    case _ => self.cogroup(other, partitioner)
  }

  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other1, other2, partitioner))
    }
    case _ => self.cogroup(other1, other2, partitioner)
  }

  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other1, other2, other3))
    }
    case _ => self.cogroup(other1, other2, other3)
  }

  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other))
    }
    case _ => self.cogroup(other)
  }

  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other1, other2))
    }
    case _ => self.cogroup(other1, other2)
  }

  def cogroup[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other, numPartitions))
    }
    case _ => self.cogroup(other, numPartitions)
  }

  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other1, other2, numPartitions))
    }
    case _ => self.cogroup(other1, other2, numPartitions)
  }

  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.cogroup(other1, other2, other3, numPartitions))
    }
    case _ => self.cogroup(other1, other2, other3, numPartitions)
  }

  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.groupWith(other))
    }
    case _ => self.groupWith(other)
  }

  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.groupWith(other1, other2))
    }
    case _ => self.groupWith(other1, other2)
  }

  def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.groupWith(other1, other2, other3))
    }
    case _ => self.groupWith(other1, other2, other3)
  }

  def subtractByKey[W](other: RDD[(K, W)])(implicit evidence$4: ClassTag[W]): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.subtractByKey(other))
    }
    case _ => self.subtractByKey(other)
  }

  def subtractByKey[W](other: RDD[(K, W)], numPartitions: Int)(implicit evidence$5: ClassTag[W]): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.subtractByKey(other, numPartitions))
    }
    case _ => self.subtractByKey(other, numPartitions)
  }

  def subtractByKey[W](other: RDD[(K, W)], p: Partitioner)(implicit evidence$6: ClassTag[W]): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.subtractByKey(other, p))
    }
    case _ => self.subtractByKey(other, p)
  }

  def lookup(key: K): Seq[V] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      self.lookup(key)
    }
    case _ => self.lookup(key)
  }

  def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      setRecorder(self, fm.runtimeClass).saveAsHadoopFile(path)
    }
    case _ => self.saveAsHadoopFile(path)
  }

  def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String, codec: Class[_ <: CompressionCodec])(implicit fm: ClassTag[F]): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      setRecorder(self, fm.runtimeClass).saveAsHadoopFile(path, codec)
    }
    case _ => self.saveAsHadoopFile(path, codec)
  }

  def saveAsNewAPIHadoopFile[F <: mapreduce.OutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      setRecorder(self, fm.runtimeClass).saveAsNewAPIHadoopFile(path)
    }
    case _ => self.saveAsNewAPIHadoopFile(path)
  }

  def saveAsNewAPIHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: mapreduce.OutputFormat[_, _]], conf: Configuration): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      setRecorder(self, outputFormatClass).saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
    }
    case _ => self.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_, _]], codec: Class[_ <: CompressionCodec]): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      setRecorder(self, outputFormatClass).saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, codec)
    }
    case _ => self.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, codec)
  }

  def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_, _]], conf: JobConf, codec: Option[Class[_ <: CompressionCodec]]): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      setRecorder(self, outputFormatClass).saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, conf, codec)
    }
    case _ => self.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, conf, codec)
  }

  def saveAsNewAPIHadoopDataset(conf: Configuration): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      self.saveAsNewAPIHadoopDataset(conf)
    }
    case _ => self.saveAsNewAPIHadoopDataset(conf)
  }

  def saveAsHadoopDataset(conf: JobConf): Unit = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      self.saveAsHadoopDataset(conf)
    }
    case _ => self.saveAsHadoopDataset(conf)
  }

  def keys: RDD[K] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.keys)
    }
    case _ => self.keys
  }

  def values: RDD[V] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.values)
    }
    case _ => self.values
  }

}

object InstrumentedPairRDDFunctions {

  private def setRecorder[B: ClassTag](rdd: RDD[B], outputFormatClass: Class[_]): RDD[B] = {
    // We do this map operation to ensure that the registry is populated (in thread local storage)
    // for the output format to use. This works only because Spark combines the map operation and the subsequent
    // call to saveAs*HadoopFile into a single task, which is executed in a single thread. This is a bit of
    // a nasty hack, but is the only option for instrumenting the output format until SPARK-3051 is fixed.
    // We uninstrument the RDD before calling map, as we don't want this map operation to be recorded (it is internal
    // to the instrumentation)
    val uninstrumentedRDD = rdd match { case instrumentedRDD: InstrumentedRDD[B] => instrumentedRDD.unInstrument() case _ => rdd }
    val recorder = functionRecorder().recorder
    val mappedRDD = uninstrumentedRDD.map(setRecorderMapFunction(_, outputFormatClass, recorder))
    rdd match { case instrumentedRDD: InstrumentedRDD[B] => mappedRDD.instrument() case _ => mappedRDD }
  }

  private def setRecorderMapFunction[B](item: B, outputFormatClass: Class[_], recorder: Option[MetricsRecorder]): B = {
    // InstrumentedOutputFormat is responsible for removing the thread-local again, so we need to ensure that this
    // is only done if we are actually using an InstrumentedOutputFormat.
    if (classOf[InstrumentedOutputFormat[_, _]].isAssignableFrom(outputFormatClass)) {
      Metrics.Recorder.value = recorder
    }
    item
  }

}
