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
package org.bdgenomics.adam.rdd

import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.formats.avro.{ Contig, RecordGroupMetadata }
import org.bdgenomics.utils.cli.SaveArgs

trait GenomicRDD[T] {

  val rdd: RDD[T]

  val sequences: SequenceDictionary
}

trait MultisampleGenomicRDD[T] extends GenomicRDD[T] {

  val samples: Seq[String]
}

abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest] extends AvroGenomicRDD[T]
    with MultisampleGenomicRDD[T] {

  override protected def saveMetadata(filePath: String) {

    // get file to write to
    val samplesAsAvroRgs = samples.map(s => {
      RecordGroupMetadata.newBuilder()
        .setSample(s)
        .setName(s)
        .build()
    })
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      RecordGroupMetadata.SCHEMA$,
      samplesAsAvroRgs)

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }
}

abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest] extends ADAMRDDFunctions[T]
    with GenomicRDD[T] {

  /**
   * Called in saveAsParquet after saving RDD to Parquet to save metadata.
   *
   * Writes any necessary metadata to disk. If not overridden, writes the
   * sequence dictionary to disk as Avro.
   *
   * @param args Arguments for saving file to disk.
   */
  protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }

  /**
   * Saves RDD as a directory of Parquet files.
   *
   * The RDD is written as a directory of Parquet files, with
   * Parquet configuration described by the input param args.
   * The provided sequence dictionary is written at args.outputPath/_seqdict.avro
   * as Avro binary.
   *
   * @param args Save configuration arguments.
   */
  def saveAsParquet(args: SaveArgs) {
    saveAsParquet(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding
    )
  }

  def saveAsParquet(
    filePath: String,
    blockSize: Int = 128 * 1024 * 1024,
    pageSize: Int = 1 * 1024 * 1024,
    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
    disableDictionaryEncoding: Boolean = false) {
    saveRddAsParquet(filePath,
      blockSize,
      pageSize,
      compressCodec,
      disableDictionaryEncoding)
    saveMetadata(filePath)
  }
}

trait Unaligned {

  val sequences = SequenceDictionary.empty
}
