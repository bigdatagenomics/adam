/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

class AdamContextSuite extends SparkFunSuite {

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads : RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    assert( reads.count() === 20 )
  }
}


