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
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.formats.avro.{ Contig, RecordGroupMetadata, Sample }
import org.bdgenomics.utils.cli.SaveArgs
import scala.reflect.ClassTag

private[rdd] class JavaSaveArgs(var outputPath: String,
                                var blockSize: Int = 128 * 1024 * 1024,
                                var pageSize: Int = 1 * 1024 * 1024,
                                var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                                var disableDictionaryEncoding: Boolean = false,
                                var asSingleFile: Boolean = false) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
}

trait GenomicRDD[T, U <: GenomicRDD[T, U]] {

  val rdd: RDD[T]

  val sequences: SequenceDictionary

  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  def transform(tFn: RDD[T] => RDD[T]): U = {
    replaceRdd(tFn(rdd))
  }

  protected def replaceRdd(newRdd: RDD[T]): U

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  protected def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
    rdd.flatMap(elem => {
      getReferenceRegions(elem).map(r => (r, elem))
    })
  }

  def filterByOverlappingRegion(query: ReferenceRegion): U = {
    replaceRdd(rdd.filter(elem => {

      // where can this item sit?
      val regions = getReferenceRegions(elem)

      // do any of these overlap with our query region?
      regions.exists(_.overlaps(query))
    }))
  }

  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](BroadcastRegionJoin.partitionAndJoin(flattenRddByRegions(),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                              optPartitions: Option[Int] = None)(
                                                                                implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](
      ShuffleRegionJoin(endSequences, partitions).partitionAndJoin(flattenRddByRegions(),
        genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }
}

private case class GenericGenomicRDD[T](rdd: RDD[T],
                                        sequences: SequenceDictionary,
                                        regionFn: T => Seq[ReferenceRegion]) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  protected def replaceRdd(newRdd: RDD[T]): GenericGenomicRDD[T] = {
    copy(rdd = newRdd)
  }

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
    regionFn(elem)
  }
}

trait MultisampleGenomicRDD[T, U <: MultisampleGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  val samples: Seq[Sample]
}

abstract class AvroReadGroupGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroReadGroupGenomicRDD[T, U]] extends AvroGenomicRDD[T, U] {

  val recordGroups: RecordGroupDictionary

  override protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)

    // convert record group to avro and save
    val rgMetadata = recordGroups.recordGroups
      .map(_.toMetadata)
    saveAvro("%s/_rgdict.avro".format(filePath),
      rdd.context,
      RecordGroupMetadata.SCHEMA$,
      rgMetadata)
  }
}

abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: MultisampleAvroGenomicRDD[T, U]] extends AvroGenomicRDD[T, U]
    with MultisampleGenomicRDD[T, U] {

  override protected def saveMetadata(filePath: String) {

    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      Sample.SCHEMA$,
      samples)

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }
}

abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroGenomicRDD[T, U]] extends ADAMRDDFunctions[T]
    with GenomicRDD[T, U] {

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

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   *   Default is false.
   */
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

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   */
  def saveAsParquet(
    filePath: java.lang.String,
    blockSize: java.lang.Integer,
    pageSize: java.lang.Integer,
    compressCodec: CompressionCodecName,
    disableDictionaryEncoding: java.lang.Boolean) {
    saveAsParquet(
      new JavaSaveArgs(filePath,
        blockSize = blockSize,
        pageSize = pageSize,
        compressionCodec = compressCodec,
        disableDictionaryEncoding = disableDictionaryEncoding))
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   */
  def saveAsParquet(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }
}

trait Unaligned {

  val sequences = SequenceDictionary.empty
}
