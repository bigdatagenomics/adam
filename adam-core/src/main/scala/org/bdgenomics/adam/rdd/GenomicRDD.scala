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

import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import java.nio.file.Paths
import htsjdk.samtools.ValidationStringency
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkFiles
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.sets._
import org.bdgenomics.formats.avro.{
  Contig,
  RecordGroup => RecordGroupMetadata,
  Sample
}
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.utils.interval.array.IntervalArray
import org.bdgenomics.utils.misc.Logging
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.math.min
import scala.reflect.ClassTag

private[rdd] class JavaSaveArgs(var outputPath: String,
                                var blockSize: Int = 128 * 1024 * 1024,
                                var pageSize: Int = 1 * 1024 * 1024,
                                var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                                var disableDictionaryEncoding: Boolean = false,
                                var asSingleFile: Boolean = false,
                                var disableFastConcat: Boolean = false) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var deferMerging = false
}

private[rdd] object GenomicRDD {

  /**
   * Replaces file references in a command.
   *
   * @see pipe
   * @param cmd Command to split and replace references in.
   * @param files List of paths to files.
   * @return Returns a split up command string, with file paths subbed in.
   */
  def processCommand(cmd: String,
                     files: Seq[String]): List[String] = {
    val filesWithIndex: Seq[(String, String)] = files.zipWithIndex
      .map(p => {
        val (file, index) = p
        ("$%d".format(index), file)
      }).reverse
    val rootPath: (String, String) = ("$root",
      Paths.get(SparkFiles.getRootDirectory())
      .toAbsolutePath.toString)
    val filesAndPath: Seq[(String, String)] = filesWithIndex ++ Seq(rootPath)

    @tailrec def replaceEscapes(cmd: String,
                                iter: Iterator[(String, String)]): String = {
      if (!iter.hasNext) {
        cmd
      } else {
        val (idx, file) = iter.next
        val newCmd = cmd.replace(idx, file)
        replaceEscapes(newCmd, iter)
      }
    }

    cmd.split(" ")
      .map(s => {
        replaceEscapes(s, filesAndPath.toIterator)
      }).toList
  }
}

/**
 * A trait that wraps an RDD of genomic data with helpful metadata.
 *
 * @tparam T The type of the data in the wrapped RDD.
 * @tparam U The type of this GenomicRDD.
 */
trait GenomicRDD[T, U <: GenomicRDD[T, U]] extends Logging {

  /**
   * The RDD of genomic data that we are wrapping.
   */
  val rdd: RDD[T]

  /**
   * The sequence dictionary describing the reference assembly this dataset is
   * aligned to.
   */
  val sequences: SequenceDictionary

  /**
   * The underlying RDD of genomic data, as a JavaRDD.
   */
  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  /**
   * Unions together multiple genomic RDDs.
   *
   * @param rdds RDDs to union with this RDD.
   */
  def union(rdds: U*): U

  /**
   * Applies a function that transforms the underlying RDD into a new RDD.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transform(tFn: RDD[T] => RDD[T]): U = {
    replaceRdd(tFn(rdd))
  }

  val isSorted: Boolean

  private[rdd] lazy val partitionMap = PartitionMap(this)

  assert(partitionMap.isEmpty ||
    partitionMap.exists(_.length == rdd.partitions.length),
    "Partition map length differs from number of partitions.")

  /**
   * Repartitions all data in rdd and distributes it as evenly as possible
   * into the number of partitions provided.
   *
   * @param partitions the number of partitions to repartition this rdd into
   * @return a new repartitioned GenomicRDD
   */
  private[rdd] def evenlyRepartition(partitions: Int)(implicit tTag: ClassTag[T]): U = {
    require(isSorted, "Cannot evenly repartition an unsorted RDD.")
    val count = rdd.count
    // we don't want a bunch of empty partitions, so we will just use count in
    // the case the user wants more partitions than rdd records.
    val finalPartitionNumber = min(count, partitions)
    // the average number of records on each node will help us evenly repartition
    val average = count.toDouble / finalPartitionNumber

    val finalPartitionedRDD =
      flattenRddByRegions()
        .zipWithIndex
        .mapPartitions(iter => {
          // divide the global index by the average to get the destination
          // partition number
          iter.map(_.swap).map(f =>
            ((f._2._1, (f._1 / average).toInt), f._2._2))
        }, preservesPartitioning = true)
        .repartitionAndSortWithinPartitions(
          ManualRegionPartitioner(finalPartitionNumber.toInt))

    replaceRdd(finalPartitionedRDD.values, isSorted = true)
  }

  /**
   * If the specified validation strategy is STRICT, throw an exception,
   * if LENIENT, log a warning, otherwise does nothing.
   *
   * @throws IllegalArgumentException If stringency is STRICT.
   * @param message The error or warning message.
   * @param stringency The validation stringency.
   */
  private def throwWarnOrNone[K](message: String,
                                 stringency: ValidationStringency): Option[K] = {
    stringency match {
      case ValidationStringency.STRICT => {
        throw new IllegalArgumentException(message)
      }
      case ValidationStringency.LENIENT => log.warn(message)
      case _                            =>
    }
    None
  }

  /**
   * Sorts our genome aligned data by reference positions, with contigs ordered
   * by index.
   *
   * @param partitions The number of partitions for the new RDD.
   * @param stringency The level of ValidationStringency to enforce.
   * @return Returns a new RDD containing sorted data.
   * @note Uses ValidationStringency to handle unaligned or where objects align
   *   to multiple positions.
   * @see sortLexicographically
   */
  def sort(partitions: Int = rdd.partitions.length,
           stringency: ValidationStringency = ValidationStringency.STRICT)(
             implicit tTag: ClassTag[T]): U = {

    require(sequences.hasSequenceOrdering,
      "Sequence Dictionary does not have ordering defined.")

    replaceRdd(rdd.flatMap(elem => {
      val coveredRegions = getReferenceRegions(elem)

      // We don't use ValidationStringency here because multimapped elements
      // break downstream methods.
      require(coveredRegions.size == 1,
        "Cannot sort RDD containing a multimapped element. %s covers %s.".format(
          elem, coveredRegions.mkString(",")))

      if (coveredRegions.isEmpty) {
        throwWarnOrNone[((Int, Long), T)](
          "Cannot sort RDD containing an unmapped element %s.".format(elem),
          stringency)
      } else {
        val contigName = coveredRegions.head.referenceName
        val sr = sequences(contigName)

        if (sr.isEmpty) {
          throwWarnOrNone[((Int, Long), T)](
            "Element %s has contig name %s not in dictionary %s.".format(
              elem, contigName, sequences),
            stringency)
        } else {
          Some(((sr.get.referenceIndex.get, coveredRegions.head.start), elem))
        }
      }
    }).sortByKey(ascending = true, numPartitions = partitions)
      .values)
  }

  /**
   * Sorts our genome aligned data by reference positions, with contigs ordered
   * lexicographically.
   *
   * @param partitions The number of partitions for the new RDD.
   * @param storageLevel The level at which to persist the resulting RDD.
   * @param stringency The level of ValidationStringency to enforce.
   * @return Returns a new RDD containing sorted data.
   * @note Uses ValidationStringency to handle data that is unaligned or where objects
   *   align to multiple positions.
   * @see sort
   */
  def sortLexicographically(partitions: Int = rdd.partitions.length,
                            storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                            stringency: ValidationStringency = ValidationStringency.STRICT)(
                              implicit tTag: ClassTag[T]): U = {

    replaceRdd(rdd.sortBy(elem => {
      val coveredRegions = getReferenceRegions(elem)

      // We don't use ValidationStringency here because multimapped elements
      // break downstream methods.
      require(coveredRegions.size == 1,
        "Cannot sort RDD containing a multimapped element. %s covers %s."
          .format(elem, coveredRegions.mkString(",")))

      if (coveredRegions.isEmpty) {
        throwWarnOrNone[(ReferenceRegion)](
          "Cannot sort RDD containing an unmapped element %s.".format(elem),
          stringency)
      } else {
        Some(coveredRegions.head)
      }
    }, ascending = true, numPartitions = partitions), isSorted = true)
  }

  /**
   * Pipes genomic data to a subprocess that runs in parallel using Spark.
   *
   * Files are substituted in to the command with a $x syntax. E.g., to invoke
   * a command that uses the first file from the files Seq, use $0. To access
   * the path to the directory where the files are copied, use $root.
   *
   * Pipes require the presence of an InFormatterCompanion and an OutFormatter
   * as implicit values. The InFormatterCompanion should be a singleton whose
   * apply method builds an InFormatter given a specific type of GenomicRDD.
   * The implicit InFormatterCompanion yields an InFormatter which is used to
   * format the input to the pipe, and the implicit OutFormatter is used to
   * parse the output from the pipe.
   *
   * @param cmd Command to run.
   * @param files Files to make locally available to the commands being run.
   *   Default is empty.
   * @param environment A map containing environment variable/value pairs to set
   *   in the environment for the newly created process. Default is empty.
   * @param flankSize Number of bases to flank each command invocation by.
   * @return Returns a new GenomicRDD of type Y.
   * @tparam X The type of the record created by the piped command.
   * @tparam Y A GenomicRDD containing X's.
   * @tparam V The InFormatter to use for formatting the data being piped to the
   *   command.
   */
  def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                                files: Seq[String] = Seq.empty,
                                                                environment: Map[String, String] = Map.empty,
                                                                flankSize: Int = 0)(implicit tFormatterCompanion: InFormatterCompanion[T, U, V],
                                                                                    xFormatter: OutFormatter[X],
                                                                                    convFn: (U, RDD[X]) => Y,
                                                                                    tManifest: ClassTag[T],
                                                                                    xManifest: ClassTag[X]): Y = {

    // TODO: support broadcasting files
    files.foreach(f => {
      rdd.context.addFile(f)
    })

    // make formatter
    val tFormatter: V = tFormatterCompanion.apply(this.asInstanceOf[U])

    // make bins
    val seqLengths = sequences.records.toSeq.map(rec => (rec.name, rec.length)).toMap
    val totalLength = seqLengths.values.sum
    val bins = GenomeBins(totalLength / rdd.partitions.size, seqLengths)

    // if the input rdd is mapped, then we need to repartition
    val partitionedRdd = if (sequences.records.size > 0) {
      // get region covered, expand region by flank size, and tag with bins
      val binKeyedRdd = rdd.flatMap(r => {

        // get regions and expand
        val regions = getReferenceRegions(r).map(_.pad(flankSize))

        // get all the bins this record falls into
        val recordBins = regions.flatMap(rr => {
          (bins.getStartBin(rr) to bins.getEndBin(rr)).map(b => (rr, b))
        })

        // key the record by those bins and return
        // TODO: this should key with the reference region corresponding to a bin
        recordBins.map(b => (b, r))
      })

      // repartition yonder our data
      binKeyedRdd
        .repartitionAndSortWithinPartitions(
          ManualRegionPartitioner(bins.numBins))
        .values
    } else {
      rdd
    }

    // are we in local mode?
    val isLocal = partitionedRdd.context.isLocal

    // call map partitions and pipe
    val pipedRdd = partitionedRdd.mapPartitions(iter => {

      // get files
      // from SPARK-3311, SparkFiles doesn't work in local mode.
      // so... we'll bypass that by checking if we're running in local mode.
      // sigh!
      val locs = if (isLocal) {
        files
      } else {
        files.map(f => {
          SparkFiles.get(new Path(f).getName())
        })
      }

      // split command and create process builder
      val finalCmd = GenomicRDD.processCommand(cmd, locs)
      val pb = new ProcessBuilder(finalCmd)
      pb.redirectError(ProcessBuilder.Redirect.INHERIT)

      // add environment variables to the process builder
      val pEnv = pb.environment()
      environment.foreach(kv => {
        val (k, v) = kv
        pEnv.put(k, v)
      })

      // start underlying piped command
      val process = pb.start()
      val os = process.getOutputStream()
      val is = process.getInputStream()

      // wrap in formatter and run as a thread
      val ifr = new InFormatterRunner[T, U, V](iter, tFormatter, os)
      new Thread(ifr).start()

      // wrap out formatter
      new OutFormatterRunner[X, OutFormatter[X]](xFormatter,
        is,
        process,
        finalCmd)
    })

    // build the new GenomicRDD
    val newRdd = convFn(this.asInstanceOf[U], pipedRdd)

    // if the original rdd was aligned and the final rdd is aligned, then we must filter
    if (newRdd.sequences.isEmpty ||
      sequences.isEmpty) {
      newRdd
    } else {
      def filterPartition(idx: Int, iter: Iterator[X]): Iterator[X] = {

        // get the region for this partition
        val region = bins.invert(idx)

        // map over the iterator and filter out any items that don't belong
        iter.filter(x => {

          // get the regions for x
          val regions = newRdd.getReferenceRegions(x)

          // are there any regions that overlap our current region
          !regions.forall(!_.overlaps(region))
        })
      }

      // run a map partitions with index and discard all items that fall
      // outside of their own partition's region bound
      newRdd.transform(_.mapPartitionsWithIndex(filterPartition))
    }
  }

  protected def replaceRdd(
    newRdd: RDD[T],
    isSorted: Boolean = false): U

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  private[rdd] def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
    rdd.flatMap(elem => {
      getReferenceRegions(elem).map(r => (r, elem))
    })
  }

  /**
   * Runs a filter that selects data in the underlying RDD that overlaps a
   * single genomic region.
   *
   * @param query The region to query for.
   * @return Returns a new GenomicRDD containing only data that overlaps the
   *   query region.
   */
  def filterByOverlappingRegion(query: ReferenceRegion): U = {
    replaceRdd(rdd.filter(elem => {

      // where can this item sit?
      val regions = getReferenceRegions(elem)

      // do any of these overlap with our query region?
      regions.exists(_.overlaps(query))
    }), isSorted)
  }

  /**
   * Runs a filter that selects data in the underlying RDD that overlaps
   * several genomic regions.
   *
   * @param querys The regions to query for.
   * @return Returns a new GenomicRDD containing only data that overlaps the
   *   querys region.
   */
  def filterByOverlappingRegions(querys: Iterable[ReferenceRegion]): U = {
    replaceRdd(rdd.filter(elem => {

      val regions = getReferenceRegions(elem)

      querys.map(query => {
        regions.exists(_.overlaps(query))
      }).fold(false)((a, b) => a || b)
    }), isSorted)
  }

  protected def buildTree(
    rdd: RDD[(ReferenceRegion, T)])(
      implicit tTag: ClassTag[T]): IntervalArray[ReferenceRegion, T]

  def broadcast()(
    implicit tTag: ClassTag[T]): Broadcast[IntervalArray[ReferenceRegion, T]] = {
    rdd.context.broadcast(buildTree(flattenRddByRegions()))
  }

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   * @see broadcastRegionJoinAgainst
   */
  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenomicRDD[(T, X), Z] = InnerBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](InnerTreeRegionJoin[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a broadcast inner join between this RDD and data that has been broadcast.
   *
   * In a broadcast join, the left side of the join (broadcastTree) is broadcast to
   * to all the nodes in the cluster. The key equality
   * function used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped. As compared to broadcastRegionJoin, this function allows the
   * broadcast object to be reused across multiple joins.
   *
   * @note This function differs from other region joins as it treats the calling RDD
   *   as the right side of the join, and not the left.
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   * @see broadcastRegionJoin
   */
  def broadcastRegionJoinAgainst[X, Z <: GenomicRDD[(X, T), Z]](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(X, T), Z] = InnerBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(X, T)](InnerTreeRegionJoin[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => { getReferenceRegions(kv._2) }) // FIXME
      .asInstanceOf[GenomicRDD[(X, T), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left RDD that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left RDD, it will be paired with a `None`
   * in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   * @see rightOuterBroadcastRegionJoin
   */
  def rightOuterBroadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenomicRDD[(Option[T], X), Z] = RightOuterBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](RightOuterTreeRegionJoin[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Option[T], X), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and data that has been broadcast.
   *
   * In a broadcast join, the left side of the join (broadcastTree) is broadcast to
   * to all the nodes in the cluster. The key equality
   * function used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left table that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left table, it will be paired with a `None`
   * in the product of the join. As compared to broadcastRegionJoin, this function allows the
   * broadcast object to be reused across multiple joins.
   *
   * @note This function differs from other region joins as it treats the calling RDD
   *   as the right side of the join, and not the left.
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   * @see rightOuterBroadcastRegionJoin
   */
  def rightOuterBroadcastRegionJoinAgainst[X, Z <: GenomicRDD[(Option[X], T), Z]](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[X], T), Z] = RightOuterBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(Option[X], T)](RightOuterTreeRegionJoin[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => {
        getReferenceRegions(kv._2) // FIXME
      })
      .asInstanceOf[GenomicRDD[(Option[X], T), Z]]
  }

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   * @see broadcastRegionJoinAgainstAndGroupByRight
   */
  def broadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Iterable[T], X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T],
    xTag: ClassTag[X],
    itxTag: ClassTag[(Iterable[T], X)]): GenomicRDD[(Iterable[T], X), Z] = BroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[T], X)](InnerTreeRegionJoinAndGroupByRight[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { (kv._1.flatMap(getReferenceRegions) ++ genomicRdd.getReferenceRegions(kv._2)).toSeq })
      .asInstanceOf[GenomicRDD[(Iterable[T], X), Z]]
  }

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left side of the join (broadcastTree) is broadcast to
   * to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped. As compared to broadcastRegionJoin, this function allows
   * the broadcast object to be reused across multiple joins.
   *
   * @note This function differs from other region joins as it treats the calling RDD
   *   as the right side of the join, and not the left.
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   * @see broadcastRegionJoinAndGroupByRight
   */
  def broadcastRegionJoinAgainstAndGroupByRight[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Iterable[X], T), Z]](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Iterable[X], T), Z] = BroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[X], T)](InnerTreeRegionJoinAndGroupByRight[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => { getReferenceRegions(kv._2).toSeq })
      .asInstanceOf[GenomicRDD[(Iterable[X], T), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left side of the join (broadcastTree) is broadcast to
   * to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left RDD that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left RDD, it will be paired with a `None`
   * in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   * @see rightOuterBroadcastRegionJoinAgainstAndGroupByRight
   */
  def rightOuterBroadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Iterable[T], X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T],
    xTag: ClassTag[X],
    itxTag: ClassTag[(Iterable[T], X)]): GenomicRDD[(Iterable[T], X), Z] = RightOuterBroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[T], X)](RightOuterTreeRegionJoinAndGroupByRight[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Iterable[T], X), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left side of the join (broadcastTree) is broadcast to
   * to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left table that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left table, it will be paired with a `None`
   * in the product of the join. As compared to broadcastRegionJoin, this
   * function allows the broadcast object to be reused across multiple joins.
   *
   * @note This function differs from other region joins as it treats the calling RDD
   *   as the right side of the join, and not the left.
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   * @see rightOuterBroadcastRegionJoinAndGroupByRight
   */
  def rightOuterBroadcastRegionJoinAgainstAndGroupByRight[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Iterable[X], T), Z]](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Iterable[X], T), Z] = RightOuterBroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[X], T)](RightOuterTreeRegionJoinAndGroupByRight[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => getReferenceRegions(kv._2).toSeq)
      .asInstanceOf[GenomicRDD[(Iterable[X], T), Z]]
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param threshold The threshold for the join. In the case of 0, the join
   *   requires overlap to join. Otherwise records must be within the threshold
   *   distance to be joined.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    threshold: Long = 0L)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenomicRDD[(T, X), Z] = InnerShuffleJoin.time {

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, X)](
      InnerShuffleRegionJoinOnGenomicRDD[T, U, X, Y](this, genomicRdd, threshold, optPartitions)
        .compute(),
      combinedSequences,
      kv => {
        getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2)
      }).asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a right outer join, all values in the
   * left RDD that do not overlap a value from the right RDD are dropped.
   * If a value from the right RDD does not overlap any values in the left
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param threshold The threshold for the join. In the case of 0, the join
   *   requires overlap to join. Otherwise records must be within the threshold
   *   distance to be joined.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    threshold: Long = 0L)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenomicRDD[(Option[T], X), Z] = RightOuterShuffleJoin.time {

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], X)](
      RightOuterShuffleRegionJoin[T, U, X, Y](this, genomicRdd, threshold, optPartitions)
        .compute(),
      combinedSequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      }).asInstanceOf[GenomicRDD[(Option[T], X), Z]]
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param threshold The threshold for the join. In the case of 0, the join
   *   requires overlap to join. Otherwise records must be within the threshold
   *   distance to be joined.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Option[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    threshold: Long = 0L)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Option[X])]): GenomicRDD[(T, Option[X]), Z] = LeftOuterShuffleJoin.time {

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, Option[X])](
      LeftOuterShuffleRegionJoin[T, U, X, Y](this, genomicRdd, threshold, optPartitions)
        .compute(),
      combinedSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten ++
          getReferenceRegions(kv._1)
      }).asInstanceOf[GenomicRDD[(T, Option[X]), Z]]
  }

  /**
   * Performs a sort-merge full outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a full outer join, if a value from either
   * RDD does not overlap any values in the other RDD, it will be paired with
   * a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param threshold The threshold for the join. In the case of 0, the join
   *   requires overlap to join. Otherwise records must be within the threshold
   *   distance to be joined.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Option[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    threshold: Long = 0L)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otoxTag: ClassTag[(Option[T], Option[X])]): GenomicRDD[(Option[T], Option[X]), Z] = FullOuterShuffleJoin.time {

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], Option[X])](
      FullOuterShuffleRegionJoin[T, U, X, Y](this, genomicRdd, threshold, optPartitions)
        .compute(),
      combinedSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v)),
          kv._1.map(v => getReferenceRegions(v))).flatten.flatten
      }).asInstanceOf[GenomicRDD[(Option[T], Option[X]), Z]]
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped. In the same operation,
   * we group all values by the left item in the RDD.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param threshold The threshold for the join. In the case of 0, the join
   *   requires overlap to join. Otherwise records must be within the threshold
   *   distance to be joined.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Iterable[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    threshold: Long = 0L)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      tixTag: ClassTag[(T, Iterable[X])]): GenomicRDD[(T, Iterable[X]), Z] = ShuffleJoinAndGroupByLeft.time {

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, Iterable[X])](
      InnerShuffleRegionJoinAndGroupByLeft[T, U, X, Y](this, genomicRdd, threshold, optPartitions)
        .compute(),
      combinedSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          getReferenceRegions(kv._1)).toSeq
      }).asInstanceOf[GenomicRDD[(T, Iterable[X]), Z]]
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD,
   * followed by a groupBy on the left value, if not null.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. In the same operation, we group all values by the left
   * item in the RDD. Since this is a right outer join, all values from the
   * right RDD who did not overlap a value from the left RDD are placed into
   * a length-1 Iterable with a `None` key.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param threshold The threshold for the join. In the case of 0, the join
   *   requires overlap to join. Otherwise records must be within the threshold
   *   distance to be joined.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Iterable[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    threshold: Long = 0L)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otixTag: ClassTag[(Option[T], Iterable[X])]): GenomicRDD[(Option[T], Iterable[X]), Z] = RightOuterShuffleJoinAndGroupByLeft.time {

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], Iterable[X])](
      RightOuterShuffleRegionJoinAndGroupByLeft[T, U, X, Y](this, genomicRdd, threshold, optPartitions)
        .compute(),
      combinedSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          kv._1.toSeq.flatMap(v => getReferenceRegions(v))).toSeq
      }).asInstanceOf[GenomicRDD[(Option[T], Iterable[X]), Z]]
  }
}

case class GenericGenomicRDD[T](
    rdd: RDD[T],
    sequences: SequenceDictionary,
    regionFn: T => Seq[ReferenceRegion],
    isSorted: Boolean = false)(
        implicit tTag: ClassTag[T]) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  def union(rdds: GenericGenomicRDD[T]*): GenericGenomicRDD[T] = {
    val iterableRdds = rdds.toSeq
    GenericGenomicRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      regionFn)
  }

  protected def buildTree(
    rdd: RDD[(ReferenceRegion, T)])(
      implicit tTag: ClassTag[T]): IntervalArray[ReferenceRegion, T] = {
    IntervalArray(rdd)
  }

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
    regionFn(elem)
  }

  protected def replaceRdd(
    newRdd: RDD[T],
    isSorted: Boolean = false): GenericGenomicRDD[T] = {

    copy(rdd = newRdd, isSorted = isSorted)
  }
}

/**
 * A trait describing a GenomicRDD with data from multiple samples.
 */
trait MultisampleGenomicRDD[T, U <: MultisampleGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  /**
   * The samples who have data contained in this GenomicRDD.
   */
  val samples: Seq[Sample]
}

/**
 * An abstract class describing a GenomicRDD where:
 *
 * * The data are Avro IndexedRecords.
 * * The data are associated to read groups (i.e., they are reads or fragments).
 */
abstract class AvroReadGroupGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroReadGroupGenomicRDD[T, U]] extends AvroGenomicRDD[T, U] {

  /**
   * A dictionary describing the read groups attached to this GenomicRDD.
   */
  val recordGroups: RecordGroupDictionary

  override protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    savePartitionMap(filePath)

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

/**
 * An abstract class that extends the MultisampleGenomicRDD trait, where the data
 * are Avro IndexedRecords.
 */
abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: MultisampleAvroGenomicRDD[T, U]] extends AvroGenomicRDD[T, U]
    with MultisampleGenomicRDD[T, U] {

  /**
   * The header lines attached to the file.
   */
  val headerLines: Seq[VCFHeaderLine]

  override protected def saveMetadata(filePath: String) {

    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration)

    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      Sample.SCHEMA$,
      samples)

    savePartitionMap(filePath)

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }
}

/**
 * An abstract class that extends GenomicRDD and where the underlying data are
 * Avro IndexedRecords. This abstract class provides methods for saving to
 * Parquet, and provides hooks for writing the metadata.
 */
abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroGenomicRDD[T, U]] extends ADAMRDDFunctions[T]
    with GenomicRDD[T, U] {

  /**
   * Save the partition map to the disk. This is done by adding the partition
   * map to the schema.
   *
   * @param filePath The filepath where we will save the Metadata.
   */
  protected def savePartitionMap(filePath: String): Unit = {
    if (isSorted) {
      // converting using json4s
      val jsonString = "partitionMap" -> partitionMap.get.toSeq.map(f =>
        if (f.isEmpty) {
          ("ReferenceRegion1" -> "None") ~ ("ReferenceRegion2" -> "None")
        } else {
          // we have to save the pair as ReferenceRegion1 and ReferenceRegion2 so we don't
          // lose either of them when they get converted to Maps
          ("ReferenceRegion1" -> (("referenceName" -> f.get._1.referenceName) ~
            ("start" -> f.get._1.start) ~ ("end" -> f.get._1.end))) ~
            ("ReferenceRegion2" -> (("referenceName" -> f.get._2.referenceName) ~
              ("start" -> f.get._2.start) ~ ("end" -> f.get._2.end)))
        })
      val schema = Contig.SCHEMA$
      schema.addProp("partitionMap", compact(render(jsonString)).asInstanceOf[Any])

      saveAvro("%s/_partitionMap.avro".format(filePath),
        rdd.context,
        schema,
        sequences.toAvro)
    }
  }
  /**
   * Called in saveAsParquet after saving RDD to Parquet to save metadata.
   *
   * Writes any necessary metadata to disk. If not overridden, writes the
   * sequence dictionary to disk as Avro.
   *
   * @param filePath The filepath to the file where we will save the Metadata.
   */
  protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    savePartitionMap(filePath)

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
