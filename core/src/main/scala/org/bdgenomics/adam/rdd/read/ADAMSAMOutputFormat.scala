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
package org.bdgenomics.adam.rdd.read

import htsjdk.samtools.SAMFileHeader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{ OutputFormat, RecordWriter, TaskAttemptContext }
import org.apache.spark.rdd.InstrumentedOutputFormat
import org.bdgenomics.adam.instrumentation.Timers
import org.seqdoop.hadoop_bam.{
  KeyIgnoringAnySAMOutputFormat,
  KeyIgnoringSAMRecordWriter,
  SAMFormat,
  SAMRecordWritable
}

class ADAMSAMOutputFormat[K]
    extends KeyIgnoringAnySAMOutputFormat[K](SAMFormat.valueOf("SAM")) with Serializable {

  setWriteHeader(true)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, SAMRecordWritable] = {
    val conf = context.getConfiguration()

    // where is our header file?
    val path = new Path(conf.get("org.bdgenomics.adam.rdd.read.bam_header_path"))

    // read the header file
    readSAMHeaderFrom(path, conf)

    // now that we have the header set, we need to make a record reader
    return new KeyIgnoringSAMRecordWriter(getDefaultWorkFile(context, ""),
      header,
      true,
      context)
  }
}

class InstrumentedADAMSAMOutputFormat[K] extends InstrumentedOutputFormat[K, org.seqdoop.hadoop_bam.SAMRecordWritable] {
  override def timerName(): String = Timers.WriteSAMRecord.timerName
  override def outputFormatClass(): Class[_ <: OutputFormat[K, SAMRecordWritable]] = classOf[ADAMSAMOutputFormat[K]]
}

class ADAMSAMOutputFormatHeaderLess[K]
    extends KeyIgnoringAnySAMOutputFormat[K](SAMFormat.valueOf("SAM")) with Serializable {

  setWriteHeader(false)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, SAMRecordWritable] = {
    val conf = context.getConfiguration()

    // where is our header file?
    val path = new Path(conf.get("org.bdgenomics.adam.rdd.read.bam_header_path"))

    // read the header file
    readSAMHeaderFrom(path, conf)

    // now that we have the header set, we need to make a record reader
    return new KeyIgnoringSAMRecordWriter(getDefaultWorkFile(context, ""),
      header,
      false,
      context)
  }
}

class InstrumentedADAMSAMOutputFormatHeaderLess[K] extends InstrumentedOutputFormat[K, org.seqdoop.hadoop_bam.SAMRecordWritable] {
  override def timerName(): String = Timers.WriteSAMRecord.timerName
  override def outputFormatClass(): Class[_ <: OutputFormat[K, SAMRecordWritable]] = classOf[ADAMSAMOutputFormatHeaderLess[K]]
}
