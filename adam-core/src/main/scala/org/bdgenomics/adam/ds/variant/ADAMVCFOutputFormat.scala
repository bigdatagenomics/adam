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
package org.bdgenomics.adam.ds.variant

import htsjdk.variant.vcf.{ VCFHeaderLine, VCFHeader }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapreduce.{ RecordWriter, TaskAttemptContext }
import org.bdgenomics.adam.models.SequenceDictionary
import org.seqdoop.hadoop_bam.{
  KeyIgnoringVCFOutputFormat,
  KeyIgnoringVCFRecordWriter,
  VariantContextWritable,
  VCFFormat
}

/**
 * Wrapper for Hadoop-BAM to work around requirement for no-args constructor.
 *
 * @tparam K The key type. Keys are not written.
 */
class ADAMVCFOutputFormat[K] extends KeyIgnoringVCFOutputFormat[K](VCFFormat.VCF) with Serializable {

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, VariantContextWritable] = {
    val conf = context.getConfiguration()

    // where is our header file?
    val path = new Path(conf.get("org.bdgenomics.adam.rdd.variant.vcf_header_path"))

    // read the header file
    readHeaderFrom(path, FileSystem.get(conf))

    // return record writer
    super.getRecordWriter(context)
  }
}
