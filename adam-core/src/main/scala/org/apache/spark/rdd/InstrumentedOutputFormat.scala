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

import org.apache.hadoop.mapreduce.{ JobContext, OutputCommitter, RecordWriter, TaskAttemptContext, OutputFormat => NewOutputFormat }
import org.bdgenomics.adam.instrumentation.Metrics

/**
 * Implementation of [[org.apache.hadoop.mapreduce.OutputFormat]], which instruments its
 * [[RecordWriter]]'s `write` method. Classes should extend this one and provide the class of the underlying
 * output format using the `outputFormatClass` method.
 *
 * This class is intended for use with the methods in [[InstrumentedPairRDDFunctions]] that save hadoop files
 * (`saveAs*HadoopFile`).
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