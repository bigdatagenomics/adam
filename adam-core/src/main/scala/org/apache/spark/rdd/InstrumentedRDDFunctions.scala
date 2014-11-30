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
import org.apache.hadoop.mapreduce.{ JobContext, OutputCommitter, RecordWriter, TaskAttemptContext, OutputFormat => NewOutputFormat }
import org.apache.spark.SparkContext._
import org.apache.spark.util.Utils
import org.bdgenomics.adam.instrumentation.{ Clock, Metrics, MetricsRecorder }
import scala.reflect.ClassTag

/**
 * Contains functions for instrumenting Spark RDD operations. Classes should typically extend this one and wrap the
 * specific operation that they are interested in instrumenting with the methods in this class.
 * For example, to instrument Spark's map operation:
 *
 * {{{
 * package org.apache.spark.rdd
 *
 * class MyInstrumentedRDDFunctions[T](self: RDD[T]) extends InstrumentedRDDFunctions() {
 *   def instrumentedMap[U: ClassTag](f: T => U): RDD[U] = {
 *     recordOperation {
 *       val recorder = metricsRecorder()
 *       self.map((t: T) => {recordFunction(f(t), recorder, FunctionTimers.MapFunction)})
 *     }
 *   }
 * }
 * object FunctionTimers extends Metrics {
 *   val MapFunction = timer("map function")
 * }
 * }}}
 *
 * An implicit conversion can then be provided to allow instrumentedMap to be called on RDDs:
 *
 * {{{
 * implicit def rddToInstrumentedRDD[T](rdd: RDD[T]) = new MyInstrumentedRDDFunctions(rdd)
 * }}}
 *
 * Then, if `instrumentedMap` is called instead of `map`, the RDD operation will be instrumented, along
 * well as any functions that operate on it.
 *
 * @note This class, as well as those that extend from it, need to be in the org.apache.spark.rdd package,
 *       otherwise Spark will record the incorrect call site (which in turn becomes the stage name).
 *       This can be fixed when Spark 1.1.1 is released (needs SPARK-1853)
 *
 */
abstract class InstrumentedRDDFunctions(clock: Clock = new Clock()) extends Serializable {

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
  def metricsRecorder(): Option[MetricsRecorder] = {
    val existingRegistryOption = Metrics.Recorder.value
    // Make a copy of the existing registry, as otherwise the stack will be unwound without having measured
    // the timings within the RDD operation
    if (existingRegistryOption.isDefined) Some(existingRegistryOption.get.copy()) else None
  }

  /**
   * Instruments a function call that acts on the data in an RDD. The recorder should have been obtained
   * from the `metricsRecorder` method. The passed-in `functionTimer` is used to time the function call.
   * See the class-level documentation for a usage example.
   *
   * The overhead of instrumenting a function call has been measured at around 120 nanoseconds on an Intel i7-3720QM.
   * The overhead of calling this method when no metrics are being recorded (a recorder is not defined) is negligible.
   */
  def recordFunction[B](function: => B, recorder: Option[MetricsRecorder], functionTimer: Timer): B = {
    if (recorder.isDefined) {
      Metrics.Recorder.withValue(recorder) {
        functionTimer.time(function)
      }
    } else {
      function
    }
  }

  /**
   * Version of `saveAsNewAPIHadoopFile` which instruments the `write` method of an
   * [[org.apache.hadoop.mapreduce.OutputFormat]]'s [[RecordWriter]]. This is achieved by using a special
   * output format (''InstrumentedOutputFormat'') which records the timings.
   */
  def instrumentedSaveAsNewAPIHadoopFile[K, V](rdd: RDD[(K, V)], path: String,
                                               keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: InstrumentedOutputFormat[_, _]],
                                               conf: Configuration)(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) {
    val recorder = metricsRecorder()
    // The call to the map operation here is to ensure that the registry is populated (in thread local storage)
    // for the output format to use. This works only because Spark combines the map operation and the subsequent
    // call to saveAsNewAPIHadoopFile into a single task, which is executed in a single thread. This is a bit of
    // a nasty hack, but is the only option for instrumenting the output format until SPARK-3051 is fixed.
    rdd.map(e => { if (recorder.isDefined) Metrics.Recorder.value = recorder; e })
      .saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  private def rddOperationTimer(): Timer = {
    // We can only do this because we are in an org.apache.spark package (Utils is private to Spark). When we fix that
    // we'll have to implement our own getCallSite function
    val callSite = Utils.getCallSite.shortForm
    new Timer(callSite, clock = clock, recorder = None,
      sequenceId = Some(Metrics.generateNewSequenceId()), isRDDOperation = true)
  }

}

/**
 * Implementation of [[org.apache.hadoop.mapreduce.OutputFormat]], which instruments its
 * [[RecordWriter]]'s `write` method. Classes should extend this one and provide the class of the underlying
 * output format using the `outputFormatClass` method.
 */
abstract class InstrumentedOutputFormat[K, V] extends NewOutputFormat[K, V] {

  val delegate = outputFormatClass().newInstance
  def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    new InstrumentedRecordWriter(delegate.getRecordWriter(context), timerName())
  }
  def checkOutputSpecs(context: JobContext) = {
    delegate.checkOutputSpecs(context)
  }
  def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    delegate.getOutputCommitter(context)
  }
  def outputFormatClass(): Class[_ <: NewOutputFormat[K, V]]
  def timerName(): String

}

private class InstrumentedRecordWriter[K, V](delegate: RecordWriter[K, V], timerName: String) extends RecordWriter[K, V] {

  // This value must be computed lazily, as when then record write is instantiated the registry may not be in place yet.
  // This is because the function that sets it may not have been executed yet, since Spark executes everything lazily.
  // However by the time we come to actually write the record this function must have been called.
  lazy val writeRecordTimer = new Timer(timerName, recorder = Metrics.Recorder.value)
  def write(key: K, value: V) = writeRecordTimer.time {
    delegate.write(key, value)
  }
  def close(context: TaskAttemptContext) = {
    // The recorder will have been set in the map function before we created the write, so we need to close it now
    Metrics.Recorder.value = None
    delegate.close(context)
  }

}
