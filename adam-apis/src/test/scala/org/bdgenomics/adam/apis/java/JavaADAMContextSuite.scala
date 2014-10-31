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
package org.bdgenomics.adam.apis.java

import org.apache.spark.api.java.JavaRDD._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.predicates.HighQualityReadPredicate
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class JavaADAMContextSuite extends SparkFunSuite {

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val ctx = new JavaADAMContext(sc)
    val reads: JavaAlignmentRecordRDD = ctx.adamRecordLoad[HighQualityReadPredicate](path)
    assert(reads.jrdd.count() === 20)
  }

  sparkTest("can read a small .SAM file inside of java") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[AlignmentRecord] = sc.adamLoad(path)

    val newReads: JavaAlignmentRecordRDD = JavaADAMConduit.conduit(reads)

    assert(newReads.jrdd.count() === 20)
  }
}
