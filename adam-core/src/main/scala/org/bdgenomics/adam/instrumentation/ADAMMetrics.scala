/**
 * Copyright (c) 2014. Neil Ferguson
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
package org.bdgenomics.adam.instrumentation

/**
 * Contains metrics for ADAM. Currently this consists only of metrics collected from Spark, but there may be others in the future.
 */
class ADAMMetrics {

  val sparkTaskMetrics = new TaskMetrics()

  class TaskMetrics extends SparkMetrics {
    val executorRunTime = taskTimer("Executor Run Time")
    val executorDeserializeTime = taskTimer("Executor Deserialization Time")
    val resultSerializationTime = taskTimer("Result Serialization Time")
    val duration = taskTimer("Task Duration")
  }

}
