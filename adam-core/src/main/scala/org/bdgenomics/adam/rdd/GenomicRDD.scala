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
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkFiles
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{ Function => JFunction, Function2 }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroup,
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.formats.avro.{
  Contig,
  ProcessingStep,
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
import scala.reflect.runtime.universe.TypeTag

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
   *
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

  override def toString = "%s with %d reference sequences"
    .format(getClass.getSimpleName, sequences.size)

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
   * Replaces the sequence dictionary attached to a GenomicRDD.
   *
   * @param newSequences The new sequence dictionary to attach.
   * @return Returns a new GenomicRDD with the sequences replaced.
   */
  def replaceSequences(newSequences: SequenceDictionary): U

  /**
   * Appends sequence metadata to the current RDD.
   *
   * @param sequencesToAdd The new sequences to append.
   * @return Returns a new GenomicRDD with the sequences appended.
   */
  def addSequences(sequencesToAdd: SequenceDictionary): U = {
    replaceSequences(sequences ++ sequencesToAdd)
  }

  /**
   * Appends metadata for a single sequence to the current RDD.
   *
   * @param sequenceToAdd The sequence to add.
   * @return Returns a new GenomicRDD with this sequence appended.
   */
  def addSequence(sequenceToAdd: SequenceRecord): U = {
    addSequences(SequenceDictionary(sequenceToAdd))
  }

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
   * Unions together multiple genomic RDDs.
   *
   * @param rdds RDDs to union with this RDD.
   */
  def union(rdds: java.util.List[U]): U = {
    val rddSeq: Seq[U] = rdds.toSeq
    union(rddSeq: _*)
  }

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

  /**
   * Applies a function that transforms the underlying RDD into a new RDD.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transform(tFn: JFunction[JavaRDD[T], JavaRDD[T]]): U = {
    replaceRdd(tFn.call(jrdd).rdd)
  }

  /**
   * Applies a function that transmutes the underlying RDD into a new RDD of a
   * different type.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transmute[X, Y <: GenomicRDD[X, Y]](tFn: RDD[T] => RDD[X])(
    implicit convFn: (U, RDD[X]) => Y): Y = {
    convFn(this.asInstanceOf[U], tFn(rdd))
  }

  /**
   * Applies a function that transmutes the underlying RDD into a new RDD of a
   * different type. Java friendly version.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @param convFn The conversion function used to build the final RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transmute[X, Y <: GenomicRDD[X, Y]](
    tFn: JFunction[JavaRDD[T], JavaRDD[X]],
    convFn: Function2[U, RDD[X], Y]): Y = {
    convFn.call(this.asInstanceOf[U], tFn.call(jrdd).rdd)
  }

  // The partition map is structured as follows:
  // The outer option is for whether or not there is a partition map.
  //   - This is None in the case that we don't know the bounds on each 
  //     partition.
  // The Array is the length of the number of partitions.
  // The inner option is in case there is no data on a partition.
  // The (ReferenceRegion, ReferenceRegion) tuple contains the bounds of the 
  //   partition, such that the lowest start is first and the highest end is
  //   second.
  protected val optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]

  assert(optPartitionMap == null ||
    optPartitionMap.isEmpty ||
    optPartitionMap.exists(_.length == rdd.partitions.length),
    "Partition map length differs from number of partitions.")

  def isSorted: Boolean = optPartitionMap.isDefined

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

    val newPartitionMap = finalPartitionedRDD.mapPartitions(iter =>
      getRegionBoundsFromPartition(
        iter.map(f => (f._1._1, f._2))),
      preservesPartitioning = true).collect

    replaceRdd(finalPartitionedRDD.values, Some(newPartitionMap))
  }

  /**
   * If the specified validation strategy is STRICT, throw an exception,
   * if LENIENT, log a warning, otherwise does nothing.
   *
   * @throws IllegalArgumentException If stringency is STRICT.
   *
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
   * @return Returns a new RDD containing sorted data.
   *
   * @see sortLexicographically
   */
  def sort(): U = {
    sort(partitions = rdd.partitions.length,
      stringency = ValidationStringency.STRICT)(ClassTag.AnyRef.asInstanceOf[ClassTag[T]])
  }

  /**
   * Sorts our genome aligned data by reference positions, with contigs ordered
   * by index.
   *
   * @param partitions The number of partitions for the new RDD.
   * @param stringency The level of ValidationStringency to enforce.
   * @return Returns a new RDD containing sorted data.
   *
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
      require(coveredRegions.size <= 1,
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
   * @return Returns a new RDD containing sorted data.
   *
   * @see sort
   */
  def sortLexicographically(): U = {
    sortLexicographically(storePartitionMap = false)(ClassTag.AnyRef.asInstanceOf[ClassTag[T]])
  }

  /**
   * Sorts our genome aligned data by reference positions, with contigs ordered
   * lexicographically.
   *
   * @param partitions The number of partitions for the new RDD.
   * @param storePartitionMap A Boolean flag to determine whether to store the
   *                          partition bounds from the resulting RDD.
   * @param storageLevel The level at which to persist the resulting RDD.
   * @param stringency The level of ValidationStringency to enforce.
   * @return Returns a new RDD containing sorted data.
   *
   * @note Uses ValidationStringency to handle data that is unaligned or where objects
   *   align to multiple positions.
   * @see sort
   */
  def sortLexicographically(partitions: Int = rdd.partitions.length,
                            storePartitionMap: Boolean = false,
                            storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                            stringency: ValidationStringency = ValidationStringency.STRICT)(
                              implicit tTag: ClassTag[T]): U = {

    val partitionedRdd = rdd.flatMap(elem => {
      val coveredRegions = getReferenceRegions(elem)

      // We don't use ValidationStringency here because multimapped elements
      // break downstream methods.
      require(coveredRegions.size <= 1,
        "Cannot sort RDD containing a multimapped element. %s covers %s.".format(
          elem, coveredRegions.mkString(",")))

      if (coveredRegions.isEmpty) {
        throwWarnOrNone[(ReferenceRegion, T)](
          "Cannot sort RDD containing an unmapped element %s.".format(elem),
          stringency)
      } else {
        Some(coveredRegions.head, elem)
      }
    }).sortByKey(ascending = true, numPartitions = partitions)

    partitionedRdd.persist(storageLevel)

    storePartitionMap match {
      case true => {
        val newPartitionMap = partitionedRdd.mapPartitions(iter =>
          getRegionBoundsFromPartition(iter), preservesPartitioning = true).collect

        replaceRdd(partitionedRdd.values, Some(newPartitionMap))
      }
      case false => {
        replaceRdd(partitionedRdd.values)
      }
    }
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
   *
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

  /**
   * Pipes genomic data to a subprocess that runs in parallel using Spark.
   *
   * SparkR friendly variant.
   *
   * @param cmd Command to run.
   * @param files Files to make locally available to the commands being run.
   *   Default is empty.
   * @param environment A map containing environment variable/value pairs to set
   *   in the environment for the newly created process. Default is empty.
   * @param flankSize Number of bases to flank each command invocation by.
   * @param tFormatter Class of formatter for data going into pipe command.
   * @param xFormatter Formatter for data coming out of the pipe command.
   * @param convFn The conversion function used to build the final RDD.
   * @return Returns a new GenomicRDD of type Y.
   *
   * @tparam X The type of the record created by the piped command.
   * @tparam Y A GenomicRDD containing X's.
   * @tparam V The InFormatter to use for formatting the data being piped to the
   *   command.
   */
  def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                                files: Seq[Any],
                                                                environment: java.util.Map[Any, Any],
                                                                flankSize: java.lang.Double,
                                                                tFormatter: Class[V],
                                                                xFormatter: OutFormatter[X],
                                                                convFn: Function2[U, RDD[X], Y]): Y = {
    pipe(cmd,
      files.asInstanceOf[Seq[String]].toList,
      environment.asInstanceOf[java.util.Map[String, String]],
      flankSize.toInt,
      tFormatter,
      xFormatter,
      convFn)
  }

  /**
   * Pipes genomic data to a subprocess that runs in parallel using Spark.
   *
   * Java/PySpark friendly variant.
   *
   * @param cmd Command to run.
   * @param files Files to make locally available to the commands being run.
   *   Default is empty.
   * @param environment A map containing environment variable/value pairs to set
   *   in the environment for the newly created process. Default is empty.
   * @param flankSize Number of bases to flank each command invocation by.
   * @param tFormatter Class of formatter for data going into pipe command.
   * @param xFormatter Formatter for data coming out of the pipe command.
   * @param convFn The conversion function used to build the final RDD.
   * @return Returns a new GenomicRDD of type Y.
   *
   * @tparam X The type of the record created by the piped command.
   * @tparam Y A GenomicRDD containing X's.
   * @tparam V The InFormatter to use for formatting the data being piped to the
   *   command.
   */
  def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                                files: java.util.List[String],
                                                                environment: java.util.Map[String, String],
                                                                flankSize: java.lang.Integer,
                                                                tFormatter: Class[V],
                                                                xFormatter: OutFormatter[X],
                                                                convFn: Function2[U, RDD[X], Y]): Y = {

    // get companion object for in formatter
    val tFormatterCompanion = {
      val companionType = try {
        tFormatter.getMethod("companion")
          .getReturnType
      } catch {
        case e: Throwable => {
          throw new IllegalArgumentException(
            "Failed to get companion apply method for user provided InFormatter (%s). Exception was: %s.".format(
              tFormatter.getName,
              e))
        }
      }

      val tFormatterCompanionConstructors = companionType.getDeclaredConstructors()
      val tFormatterCompanionConstructor = tFormatterCompanionConstructors.head
      tFormatterCompanionConstructor.setAccessible(true)

      tFormatterCompanionConstructor.newInstance()
        .asInstanceOf[InFormatterCompanion[T, U, V]]
    }

    pipe(cmd, files.toSeq, environment.toMap, flankSize)(
      tFormatterCompanion,
      xFormatter,
      (gRdd: U, rdd: RDD[X]) => convFn.call(gRdd, rdd),
      ClassTag.AnyRef.asInstanceOf[ClassTag[T]],
      ClassTag.AnyRef.asInstanceOf[ClassTag[X]])
  }

  protected def replaceRdd(
    newRdd: RDD[T],
    newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): U

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  protected def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
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
    }), optPartitionMap)
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

      querys.exists(query => {
        regions.exists(_.overlaps(query))
      })
    }), optPartitionMap)
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   *
   * @see broadcastRegionJoinAgainst
   */
  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenericGenomicRDD[(T, X)] = InnerBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](InnerTreeRegionJoin[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions().map(f => (f._1.pad(flankSize), f._2))),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        getReferenceRegions(kv._1).map(_.pad(-1 * flankSize)) ++
          genomicRdd.getReferenceRegions(kv._2)
      })
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
   *
   * @see broadcastRegionJoinAgainst
   */
  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenericGenomicRDD[(T, X)] = {

    broadcastRegionJoin(genomicRdd, 0L)
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
   *
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   *
   * @see broadcastRegionJoin
   */
  def broadcastRegionJoinAgainst[X](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(X, T)] = InnerBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(X, T)](InnerTreeRegionJoin[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => { getReferenceRegions(kv._2) }) // FIXME
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   *
   * @see rightOuterBroadcastRegionJoin
   */
  def rightOuterBroadcastRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenericGenomicRDD[(Option[T], X)] = RightOuterBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](RightOuterTreeRegionJoin[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions().map(f => (f._1.pad(flankSize), f._2))),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        Seq(kv._1.map(v => getReferenceRegions(v)
          .map(_.pad(-1 * flankSize)))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
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
   *
   * @see rightOuterBroadcastRegionJoin
   */
  def rightOuterBroadcastRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenericGenomicRDD[(Option[T], X)] = {

    rightOuterBroadcastRegionJoin(genomicRdd, 0L)
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
   *
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   *
   * @see rightOuterBroadcastRegionJoin
   */
  def rightOuterBroadcastRegionJoinAgainst[X](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(Option[X], T)] = RightOuterBroadcastJoin.time {

    // key the RDDs and join
    GenericGenomicRDD[(Option[X], T)](RightOuterTreeRegionJoin[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => {
        getReferenceRegions(kv._2) // FIXME
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   *
   * @see broadcastRegionJoinAgainstAndGroupByRight
   */
  def broadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      itxTag: ClassTag[(Iterable[T], X)]): GenericGenomicRDD[(Iterable[T], X)] = BroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[T], X)](InnerTreeRegionJoinAndGroupByRight[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions().map(f => (f._1.pad(flankSize), f._2))),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        (kv._1.flatMap(getReferenceRegions).map(_.pad(-1 * flankSize)) ++
          genomicRdd.getReferenceRegions(kv._2))
          .toSeq
      })
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
   *
   * @see broadcastRegionJoinAgainstAndGroupByRight
   */
  def broadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      itxTag: ClassTag[(Iterable[T], X)]): GenericGenomicRDD[(Iterable[T], X)] = {

    broadcastRegionJoinAndGroupByRight(genomicRdd, 0L)
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
   *
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   *
   * @see broadcastRegionJoinAndGroupByRight
   */
  def broadcastRegionJoinAgainstAndGroupByRight[X, Y <: GenomicRDD[X, Y]](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(Iterable[X], T)] = BroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[X], T)](InnerTreeRegionJoinAndGroupByRight[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => { getReferenceRegions(kv._2) })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   *
   * @see rightOuterBroadcastRegionJoinAgainstAndGroupByRight
   */
  def rightOuterBroadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      itxTag: ClassTag[(Iterable[T], X)]): GenericGenomicRDD[(Iterable[T], X)] = RightOuterBroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[T], X)](RightOuterTreeRegionJoinAndGroupByRight[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions().map(f => (f._1.pad(flankSize), f._2))),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        Seq(kv._1.map(v => getReferenceRegions(v)
          .map(_.pad(-1 * flankSize)))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
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
   *
   * @see rightOuterBroadcastRegionJoinAgainstAndGroupByRight
   */
  def rightOuterBroadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      itxTag: ClassTag[(Iterable[T], X)]): GenericGenomicRDD[(Iterable[T], X)] = {

    rightOuterBroadcastRegionJoinAndGroupByRight(genomicRdd, 0L)
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
   *
   * @param broadcastTree The data on the left side of the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   *
   * @see rightOuterBroadcastRegionJoinAndGroupByRight
   */
  def rightOuterBroadcastRegionJoinAgainstAndGroupByRight[X, Y <: GenomicRDD[X, Y]](
    broadcastTree: Broadcast[IntervalArray[ReferenceRegion, X]])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(Iterable[X], T)] = RightOuterBroadcastJoinAndGroupByRight.time {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[X], T)](RightOuterTreeRegionJoinAndGroupByRight[X, T]().join(
      broadcastTree,
      flattenRddByRegions()),
      sequences,
      kv => getReferenceRegions(kv._2).toSeq)
  }

  /**
   * Prepares two RDDs to be joined with any shuffleRegionJoin. This includes copartition
   * and sort of the rightRdd if necessary.
   *
   * @param genomicRdd The RDD to join to.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return a case class containing all the prepared data for ShuffleRegionJoins
   */
  private def prepareForShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None,
    flankSize: Long)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)]) = {

    val partitions = optPartitions.getOrElse(this.rdd.partitions.length)

    val (leftRdd, rightRdd) = (isSorted, genomicRdd.isSorted) match {
      case (true, _)     => (this, genomicRdd.copartitionByReferenceRegion(this, flankSize))
      case (false, true) => (copartitionByReferenceRegion(genomicRdd, flankSize), genomicRdd)
      case (false, false) => {
        val repartitionedRdd =
          sortLexicographically(storePartitionMap = true, partitions = partitions)

        (repartitionedRdd, genomicRdd.copartitionByReferenceRegion(repartitionedRdd, flankSize))
      }
    }
    (leftRdd.flattenRddByRegions().map(f => (f._1.pad(flankSize), f._2)),
      rightRdd.flattenRddByRegions())
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  private[rdd] def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenericGenomicRDD[(T, X)] = InnerShuffleJoin.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, X)](
      InnerShuffleRegionJoin[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        getReferenceRegions(kv._1).map(_.pad(-1 * flankSize)) ++
          genomicRdd.getReferenceRegions(kv._2)
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenericGenomicRDD[(T, X)] = {

    shuffleRegionJoin(genomicRdd, None, flankSize)
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      txTag: ClassTag[(T, X)]): GenericGenomicRDD[(T, X)] = {

    shuffleRegionJoin(genomicRdd, None, 0L)
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  private[rdd] def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenericGenomicRDD[(Option[T], X)] = RightOuterShuffleJoin.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], X)](
      RightOuterShuffleRegionJoin[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        Seq(kv._1.map(v => getReferenceRegions(v)
          .map(_.pad(-1 * flankSize)))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenericGenomicRDD[(Option[T], X)] = {

    rightOuterShuffleRegionJoin(genomicRdd, None, flankSize)
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otxTag: ClassTag[(Option[T], X)]): GenericGenomicRDD[(Option[T], X)] = {

    rightOuterShuffleRegionJoin(genomicRdd, None, 0L)
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  private[rdd] def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Option[X])]): GenericGenomicRDD[(T, Option[X])] = LeftOuterShuffleJoin.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, Option[X])](
      LeftOuterShuffleRegionJoin[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        getReferenceRegions(kv._1).map(_.pad(-1 * flankSize)) ++
          Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Option[X])]): GenericGenomicRDD[(T, Option[X])] = {

    leftOuterShuffleRegionJoin(genomicRdd, None, flankSize)
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Option[X])]): GenericGenomicRDD[(T, Option[X])] = {

    leftOuterShuffleRegionJoin(genomicRdd, None, 0L)
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with an empty Iterable in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optionally sets the number of output partitions. If
   *   None, the number of partitions on the resulting RDD does not change.
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  private[rdd] def leftOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Iterable[X])]): GenericGenomicRDD[(T, Iterable[X])] = LeftOuterShuffleJoin.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, Iterable[X])](
      LeftOuterShuffleRegionJoinAndGroupByLeft[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo flank from preprocessing
        getReferenceRegions(kv._1).map(_.pad(-1 * flankSize)) ++
          Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten
      })
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with an empty Iterable in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Iterable[X])]): GenericGenomicRDD[(T, Iterable[X])] = {

    leftOuterShuffleRegionJoinAndGroupByLeft(genomicRdd, None, flankSize)
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with an empty Iterable in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      toxTag: ClassTag[(T, Iterable[X])]): GenericGenomicRDD[(T, Iterable[X])] = {

    leftOuterShuffleRegionJoinAndGroupByLeft(genomicRdd, None, 0L)
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  private[rdd] def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otoxTag: ClassTag[(Option[T], Option[X])]): GenericGenomicRDD[(Option[T], Option[X])] = FullOuterShuffleJoin.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], Option[X])](
      FullOuterShuffleRegionJoin[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        Seq(kv._1.map(v => getReferenceRegions(v).map(_.pad(-1 * flankSize))),
          kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otoxTag: ClassTag[(Option[T], Option[X])]): GenericGenomicRDD[(Option[T], Option[X])] = {

    fullOuterShuffleRegionJoin(genomicRdd, None, flankSize)
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otoxTag: ClassTag[(Option[T], Option[X])]): GenericGenomicRDD[(Option[T], Option[X])] = {

    fullOuterShuffleRegionJoin(genomicRdd, None, 0L)
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  private[rdd] def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      tixTag: ClassTag[(T, Iterable[X])]): GenericGenomicRDD[(T, Iterable[X])] = ShuffleJoinAndGroupByLeft.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, Iterable[X])](
      InnerShuffleRegionJoinAndGroupByLeft[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        getReferenceRegions(kv._1)
          .map(_.pad(-1 * flankSize)) ++
          kv._2.flatMap(v => genomicRdd.getReferenceRegions(v))
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      tixTag: ClassTag[(T, Iterable[X])]): GenericGenomicRDD[(T, Iterable[X])] = {

    shuffleRegionJoinAndGroupByLeft(genomicRdd, None, flankSize)
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      tixTag: ClassTag[(T, Iterable[X])]): GenericGenomicRDD[(T, Iterable[X])] = {

    shuffleRegionJoinAndGroupByLeft(genomicRdd, None, 0L)
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  private[rdd] def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otixTag: ClassTag[(Option[T], Iterable[X])]): GenericGenomicRDD[(Option[T], Iterable[X])] = RightOuterShuffleJoinAndGroupByLeft.time {

    val (leftRddToJoin, rightRddToJoin) =
      prepareForShuffleRegionJoin(genomicRdd, optPartitions, flankSize)

    // what sequences do we wind up with at the end?
    val combinedSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], Iterable[X])](
      RightOuterShuffleRegionJoinAndGroupByLeft[T, X](leftRddToJoin, rightRddToJoin)
        .compute(),
      combinedSequences,
      kv => {
        // pad by -1 * flankSize to undo pad from preprocessing
        kv._1.toSeq.flatMap(v => getReferenceRegions(v)
          .map(_.pad(-1 * flankSize))) ++
          kv._2.flatMap(v => genomicRdd.getReferenceRegions(v))
      })
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
   * @param flankSize Sets a flankSize for the distance between elements to be
   *   joined. If set to 0, an overlap is required to join two elements.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y],
    flankSize: Long)(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otixTag: ClassTag[(Option[T], Iterable[X])]): GenericGenomicRDD[(Option[T], Iterable[X])] = {

    rightOuterShuffleRegionJoinAndGroupByLeft(genomicRdd, None, flankSize)
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T],
      xTag: ClassTag[X],
      otixTag: ClassTag[(Option[T], Iterable[X])]): GenericGenomicRDD[(Option[T], Iterable[X])] = {

    rightOuterShuffleRegionJoinAndGroupByLeft(genomicRdd, None, 0L)
  }

  /**
   * Copartitions two RDDs according to their ReferenceRegions.
   *
   * @note This is best used under the condition that (repeatedly)
   *   repartitioning is more expensive than calculating the proper location
   *   of the records of this.rdd. It requires a pass through the co-located
   *   RDD to get the correct partition(s) for each record. It will assign a
   *   record to multiple partitions if necessary.
   *
   * @param rddToCoPartitionWith The rdd to copartition to.
   * @return The newly repartitioned rdd.
   */
  private[rdd] def copartitionByReferenceRegion[X, Y <: GenomicRDD[X, Y]](
    rddToCoPartitionWith: GenomicRDD[X, Y],
    flankSize: Long = 0L)(implicit tTag: ClassTag[T], xTag: ClassTag[X]): U = {

    // if the other RDD is not sorted, we can't guarantee proper copartition
    assert(rddToCoPartitionWith.isSorted,
      "Cannot copartition with an unsorted rdd!")

    val destinationPartitionMap = rddToCoPartitionWith.optPartitionMap.get

    // number of partitions we will have after repartition
    val numPartitions = destinationPartitionMap.length

    // here we create a partition map with a single ReferenceRegion that spans
    // the entire range, however we have to handle the case where the partition
    // spans multiple referenceNames because of load balancing.
    val adjustedPartitionMapWithIndex =

      // the zipWithIndex gives us the destination partition ID
      destinationPartitionMap.flatten.zipWithIndex.map(g => {
        val (firstRegion, secondRegion, index) = (g._1._1, g._1._2, g._2)

        // in the case where we span multiple referenceNames using
        // IntervalArray.get with requireOverlap set to false will assign all
        // the remaining regions to this partition, in addition to all the
        // regions up to the start of the next partition.
        if (firstRegion.referenceName != secondRegion.referenceName) {

          // the first region is enough to represent the partition for
          // IntervalArray.get.
          (firstRegion, index)
        } else {
          // otherwise we just have the ReferenceRegion span from partition
          // lower bound to upper bound.
          // We cannot use the firstRegion bounds here because we may end up
          // dropping data if it doesn't map anywhere.
          (ReferenceRegion(
            firstRegion.referenceName,
            firstRegion.start,
            secondRegion.end),
            index)
        }
      })

    // convert to an IntervalArray for fast range query
    val partitionMapIntervals = IntervalArray(
      adjustedPartitionMapWithIndex,
      adjustedPartitionMapWithIndex.maxBy(_._1.width)._1.width,
      sorted = true)

    val finalPartitionedRDD = {
      val referenceRegionKeyedGenomicRDD = flattenRddByRegions()

      referenceRegionKeyedGenomicRDD.mapPartitions(iter => {
        iter.flatMap(f => {
          val intervals = partitionMapIntervals.get(f._1.pad(flankSize), requireOverlap = false)
          intervals.map(g => ((f._1, g._2), f._2))
        })
      }, preservesPartitioning = true)
        .repartitionAndSortWithinPartitions(
          ManualRegionPartitioner(numPartitions))
    }

    replaceRdd(finalPartitionedRDD.values, rddToCoPartitionWith.optPartitionMap)
  }

  /**
   * Gets the partition bounds from a ReferenceRegion keyed Iterator.
   *
   * @param iter The data on a given partition. ReferenceRegion keyed.
   * @return The bounds of the ReferenceRegions on that partition, in an Iterator.
   */
  private def getRegionBoundsFromPartition(
    iter: Iterator[(ReferenceRegion, T)]): Iterator[Option[(ReferenceRegion, ReferenceRegion)]] = {

    if (iter.isEmpty) {
      // This means that there is no data on the partition, so we have no bounds
      Iterator(None)
    } else {
      val firstRegion = iter.next
      val lastRegion =
        if (iter.hasNext) {
          // we have to make sure we get the full bounds of this partition, this
          // includes any extremely long regions. we include the firstRegion for
          // the case that the first region is extremely long
          (iter ++ Iterator(firstRegion)).maxBy(f => (f._1.referenceName, f._1.end, f._1.start))
        } else {
          // only one record on this partition, so this is the extent of the bounds
          firstRegion
        }
      Iterator(Some((firstRegion._1, lastRegion._1)))
    }
  }

  /**
   * Writes an RDD to disk as text and optionally merges.
   *
   * @param rdd RDD to save.
   * @param outputPath Output path to save text files to.
   * @param asSingleFile If true, combines all partition shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param optHeaderPath If provided, the header file to include.
   */
  protected def writeTextRdd[T](rdd: RDD[T],
                                outputPath: String,
                                asSingleFile: Boolean,
                                disableFastConcat: Boolean,
                                optHeaderPath: Option[String] = None) {
    if (asSingleFile) {

      // write rdd to disk
      val tailPath = "%s_tail".format(outputPath)
      rdd.saveAsTextFile(tailPath)

      // get the filesystem impl
      val fs = FileSystem.get(rdd.context.hadoopConfiguration)

      // and then merge
      FileMerger.mergeFiles(rdd.context,
        fs,
        new Path(outputPath),
        new Path(tailPath),
        disableFastConcat = disableFastConcat,
        optHeaderPath = optHeaderPath.map(p => new Path(p)))
    } else {
      assert(optHeaderPath.isEmpty)
      rdd.saveAsTextFile(outputPath)
    }
  }
}

case class GenericGenomicRDD[T](
    rdd: RDD[T],
    sequences: SequenceDictionary,
    regionFn: T => Seq[ReferenceRegion],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None)(
        implicit tTag: ClassTag[T]) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  def replaceSequences(
    newSequences: SequenceDictionary): GenericGenomicRDD[T] = {
    copy(sequences = newSequences)
  }

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
    newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): GenericGenomicRDD[T] = {

    copy(rdd = newRdd, optPartitionMap = newPartitionMap)
  }
}

/**
 * A trait describing a GenomicRDD with data from multiple samples.
 */
trait MultisampleGenomicRDD[T, U <: MultisampleGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  override def toString = "%s with %d reference sequences and %d samples"
    .format(getClass.getSimpleName, sequences.size, samples.size)

  /**
   * The samples who have data contained in this GenomicRDD.
   */
  val samples: Seq[Sample]

  /**
   * Replaces the sample metadata attached to the RDD.
   *
   * @param newSamples The new sample metadata to attach.
   * @return A GenomicRDD with new sample metadata.
   */
  def replaceSamples(newSamples: Iterable[Sample]): U

  /**
   * Adds samples to the current RDD.
   *
   * @param samplesToAdd Zero or more samples to add.
   * @return Returns a new RDD with samples added.
   */
  def addSamples(samplesToAdd: Iterable[Sample]): U = {
    replaceSamples(samples ++ samplesToAdd)
  }

  /**
   * Adds a single sample to the current RDD.
   *
   * @param sampleToAdd A single sample to add.
   * @return Returns a new RDD with this sample added.
   */
  def addSample(sampleToAdd: Sample): U = {
    addSamples(Seq(sampleToAdd))
  }
}

/**
 * A trait describing a GenomicRDD that also supports the Spark SQL APIs.
 */
trait GenomicDataset[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicRDD[T, V] {

  val uTag: TypeTag[U]

  /**
   * This data as a Spark SQL Dataset.
   */
  val dataset: Dataset[U]

  /**
   * @return This data as a Spark SQL DataFrame.
   */
  def toDF(): DataFrame = {
    dataset.toDF()
  }

  /**
   * Applies a function that transforms the underlying Dataset into a new Dataset
   * using the Spark SQL API.
   *
   * @param tFn A function that transforms the underlying RDD as a Dataset.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transformDataset(tFn: Dataset[U] => Dataset[U]): V

  /**
   * Applies a function that transforms the underlying DataFrame into a new DataFrame
   * using the Spark SQL API.
   *
   * @param tFn A function that transforms the underlying RDD as a DataFrame.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transformDataFrame(tFn: DataFrame => DataFrame)(
    implicit uTag: scala.reflect.runtime.universe.TypeTag[U]): V = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transformDataset((ds: Dataset[U]) => {
      tFn(ds.toDF()).as[U]
    })
  }

  /**
   * Applies a function that transforms the underlying DataFrame into a new DataFrame
   * using the Spark SQL API. Java-friendly variant.
   *
   * @param tFn A function that transforms the underlying RDD as a DataFrame.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transformDataFrame(tFn: JFunction[DataFrame, DataFrame]): V = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transformDataFrame(tFn.call(_))(uTag)
  }

  /**
   * Applies a function that transmutes the underlying RDD into a new RDD of a
   * different type.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transmuteDataset[X <: Product, Y <: GenomicDataset[_, X, Y]](
    tFn: Dataset[U] => Dataset[X])(
      implicit xTag: scala.reflect.runtime.universe.TypeTag[X],
      convFn: (V, Dataset[X]) => Y): Y = {
    convFn(this.asInstanceOf[V], tFn(dataset))
  }

  /**
   * Applies a function that transmutes the underlying RDD into a new RDD of a
   * different type. Java friendly variant.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transmuteDataset[X <: Product, Y <: GenomicDataset[_, X, Y]](
    tFn: JFunction[Dataset[U], Dataset[X]],
    convFn: GenomicDatasetConversion[U, V, X, Y]): Y = {
    transmuteDataset(tFn.call(_))(convFn.xTag, convFn.call(_, _))
  }

  /**
   * Applies a function that transmutes the underlying RDD into a new RDD of a
   * different type. Java friendly variant.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transmuteDataFrame[X <: Product, Y <: GenomicDataset[_, X, Y]](
    tFn: DataFrame => DataFrame)(
      implicit xTag: scala.reflect.runtime.universe.TypeTag[X],
      convFn: (V, Dataset[X]) => Y): Y = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transmuteDataset((ds: Dataset[U]) => {
      tFn(ds.toDF()).as[X]
    })
  }

  /**
   * Applies a function that transmutes the underlying RDD into a new RDD of a
   * different type. Java friendly variant.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transmuteDataFrame[X <: Product, Y <: GenomicDataset[_, X, Y]](
    tFn: JFunction[DataFrame, DataFrame],
    convFn: GenomicDatasetConversion[U, V, X, Y]): Y = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transmuteDataFrame(tFn.call(_))(convFn.xTag,
      (v: V, dsX: Dataset[X]) => {
        convFn.call(v, dsX)
      })
  }
}

trait GenomicRDDWithLineage[T, U <: GenomicRDDWithLineage[T, U]] extends GenomicRDD[T, U] {

  /**
   * The processing steps that have been applied to this GenomicRDD.
   */
  val processingSteps: Seq[ProcessingStep]

  /**
   * Replaces the processing steps attached to this RDD.
   *
   * @param newProcessingSteps The new processing steps to attach to this RDD.
   * @return Returns a new GenomicRDD with new processing lineage attached.
   */
  def replaceProcessingSteps(newProcessingSteps: Seq[ProcessingStep]): U

  /**
   * Merges a new processing record with the extant computational lineage.
   *
   * @param newProcessingStep
   * @return Returns a new GenomicRDD with new record groups merged in.
   */
  def addProcessingStep(newProcessingStep: ProcessingStep): U = {
    replaceProcessingSteps(processingSteps :+ newProcessingStep)
  }
}

/**
 * An abstract class describing a GenomicRDD where:
 *
 * * The data are Avro IndexedRecords.
 * * The data are associated to record groups (i.e., they are reads or fragments).
 */
abstract class AvroRecordGroupGenomicRDD[T <% IndexedRecord: Manifest, U <: Product, V <: AvroRecordGroupGenomicRDD[T, U, V]] extends AvroGenomicRDD[T, U, V]
    with GenomicRDDWithLineage[T, V] {

  override def toString = "%s with %d reference sequences, %d read groups, and %d processing steps"
    .format(getClass.getSimpleName, sequences.size, recordGroups.size, processingSteps.size)

  /**
   * A dictionary describing the record groups attached to this GenomicRDD.
   */
  val recordGroups: RecordGroupDictionary

  /**
   * Replaces the record groups attached to this RDD.
   *
   * @param newRecordGroups The new record group dictionary to attach.
   * @return Returns a new GenomicRDD with new record groups attached.
   */
  def replaceRecordGroups(newRecordGroups: RecordGroupDictionary): V

  /**
   * Merges a new set of record groups with the extant record groups.
   *
   * @param recordGroupsToAdd The record group dictionary to append to the
   *   extant record groups.
   * @return Returns a new GenomicRDD with new record groups merged in.
   */
  def addRecordGroups(recordGroupsToAdd: RecordGroupDictionary): V = {
    replaceRecordGroups(recordGroups ++ recordGroupsToAdd)
  }

  /**
   * Adds a single record group to the extant record groups.
   *
   * @param recordGroupToAdd The record group to append to the extant record
   *   groups.
   * @return Returns a new GenomicRDD with the new record group added.
   */
  def addRecordGroup(recordGroupToAdd: RecordGroup): V = {
    addRecordGroups(RecordGroupDictionary(Seq(recordGroupToAdd)))
  }

  /**
   * Save the record groups to disk.
   *
   * @param filePath The filepath to the file where we will save the record groups.
   */
  protected def saveRecordGroups(filePath: String): Unit = {

    // convert record group to avro and save
    val rgMetadata = recordGroups.recordGroups
      .map(_.toMetadata)

    saveAvro("%s/_rgdict.avro".format(filePath),
      rdd.context,
      RecordGroupMetadata.SCHEMA$,
      rgMetadata)
  }

  /**
   * Save the processing steps to disk.
   *
   * @param filePath The filepath to the directory within which we will save the
   *   processing step descriptions..
   */
  protected def saveProcessingSteps(filePath: String) {
    // save processing metadata
    saveAvro("%s/_processing.avro".format(filePath),
      rdd.context,
      ProcessingStep.SCHEMA$,
      processingSteps)
  }

  override protected def saveMetadata(filePath: String): Unit = {
    savePartitionMap(filePath)
    saveProcessingSteps(filePath)
    saveSequences(filePath)
    saveRecordGroups(filePath)
  }
}

/**
 * An abstract class that extends the MultisampleGenomicRDD trait, where the data
 * are Avro IndexedRecords.
 */
abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: Product, V <: MultisampleAvroGenomicRDD[T, U, V]] extends AvroGenomicRDD[T, U, V]
    with MultisampleGenomicRDD[T, V] {

  /**
   * Save the samples to disk.
   *
   * @param filePath The filepath to the file where we will save the samples.
   */
  protected def saveSamples(filePath: String): Unit = {
    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      Sample.SCHEMA$,
      samples)
  }

  override protected def saveMetadata(filePath: String): Unit = {
    savePartitionMap(filePath)
    saveSequences(filePath)
    saveSamples(filePath)
  }
}

/**
 * An abstract class that extends GenomicRDD and where the underlying data are
 * Avro IndexedRecords. This abstract class provides methods for saving to
 * Parquet, and provides hooks for writing the metadata.
 */
abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest, U <: Product, V <: AvroGenomicRDD[T, U, V]] extends ADAMRDDFunctions[T]
    with GenomicDataset[T, U, V] {

  /**
   * Save the partition map to disk. This is done by adding the partition
   * map to the schema.
   *
   * @param filePath The filepath where we will save the partition map.
   */
  protected def savePartitionMap(filePath: String): Unit = {
    if (isSorted) {
      // converting using json4s
      val jsonString = "partitionMap" -> optPartitionMap.get.toSeq.map(f =>
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
   * Save the sequence dictionary to disk.
   *
   * @param filePath The filepath where we will save the sequence dictionary.
   */
  protected def saveSequences(filePath: String): Unit = {
    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro

    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }

  /**
   * Called in saveAsParquet after saving RDD to Parquet to save metadata.
   *
   * Writes any necessary metadata to disk. If not overridden, writes the
   * sequence dictionary to disk as Avro.
   *
   * @param filePath The filepath to the file where we will save the Metadata.
   */
  protected def saveMetadata(filePath: String): Unit = {
    savePartitionMap(filePath)
    saveSequences(filePath)
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
