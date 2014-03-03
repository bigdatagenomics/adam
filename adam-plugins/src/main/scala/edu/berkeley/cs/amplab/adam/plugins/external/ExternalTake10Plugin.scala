/**
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
package edu.berkeley.cs.amplab.adam.plugins.external

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.plugins.AdamPlugin
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class ExternalTake10Plugin extends AdamPlugin[ADAMRecord, ADAMRecord] with Serializable {
   override def projection: Option[Schema] = None
   override def predicate: Option[(ADAMRecord) => Boolean] = None

   override def run(sc: SparkContext, recs: RDD[ADAMRecord]): RDD[ADAMRecord] = {
     sc.parallelize(recs.take(10))
   }
 }
