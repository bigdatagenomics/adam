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

import fi.tkk.ics.hadoop.bam.{ SAMRecordWritable, AnySAMInputFormat }
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import java.util.regex.Pattern
import net.sf.samtools.SAMFileHeader
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.StatsReportListener
import org.bdgenomics.formats.avro.{
  ADAMPileup,
  ADAMRecord,
  ADAMNucleotideContigFragment
}
import org.bdgenomics.adam.converters.{ FastaConverter, SAMRecordConverter }
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.predicates.ADAMPredicate
import org.bdgenomics.adam.projections.{
  ADAMRecordField,
  Projection,
  ADAMNucleotideContigFragmentField
}
import org.bdgenomics.adam.rich.RichADAMRecord
import org.bdgenomics.adam.util.HadoopUtil
import parquet.avro.{ AvroParquetInputFormat, AvroReadSupport }
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil
import scala.Some
import scala.collection.JavaConversions._
import scala.collection.Map
import org.bdgenomics.adam.instrumentation.ADAMMetricsListener

object ADAMContext {
  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext): ADAMContext = new ADAMContext(sc)

  // Add methods specific to ADAMRecord RDDs
  implicit def rddToADAMRecordRDD(rdd: RDD[ADAMRecord]) = new ADAMRecordRDDFunctions(rdd)

  // Add methods specific to the ADAMPileup RDDs
  implicit def rddToADAMPileupRDD(rdd: RDD[ADAMPileup]) = new ADAMPileupRDDFunctions(rdd)

  // Add methods specific to the ADAMRod RDDs
  implicit def rddToADAMRodRDD(rdd: RDD[ADAMRod]) = new ADAMRodRDDFunctions(rdd)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToADAMRDD[T <% SpecificRecord: Manifest](rdd: RDD[T]) = new ADAMRDDFunctions(rdd)

  // Add methods specific to the ADAMNucleotideContig RDDs
  implicit def rddToADAMRDD(rdd: RDD[ADAMNucleotideContigFragment]) = new ADAMNucleotideContigFragmentRDDFunctions(rdd)

  // Add implicits for the rich adam objects
  implicit def recordToRichRecord(record: ADAMRecord): RichADAMRecord = new RichADAMRecord(record)

  // implicit java to scala type conversions
  implicit def listToJavaList[A](list: List[A]): java.util.List[A] = seqAsJavaList(list)

  implicit def javaListToList[A](list: java.util.List[A]): List[A] = asScalaBuffer(list).toList

  implicit def javaSetToSet[A](set: java.util.Set[A]): Set[A] = {
    // toSet is necessary to make set immutable
    asScalaSet(set).toSet
  }

  implicit def intListToJavaIntegerList(list: List[Int]): java.util.List[java.lang.Integer] = {
    seqAsJavaList(list.map(i => i: java.lang.Integer))
  }

  implicit def charSequenceToString(cs: CharSequence): String = cs.toString

  implicit def charSequenceToList(cs: CharSequence): List[Char] = cs.toCharArray.toList

  implicit def mapToJavaMap[A, B](map: Map[A, B]): java.util.Map[A, B] = mapAsJavaMap(map)

  implicit def iterableToJavaCollection[A](i: Iterable[A]): java.util.Collection[A] = asJavaCollection(i)

  implicit def setToJavaSet[A](set: Set[A]): java.util.Set[A] = setAsJavaSet(set)

  /**
   * Creates a SparkContext that is configured for use with ADAM. Applies default serialization
   * settings.
   *
   * @param name Context name.
   * @param master URL for master.
   * @param sparkHome Path to Spark.
   * @param sparkJars JAR files to import into context.
   * @param sparkEnvVars Environment variables to set.
   * @param sparkAddStatsListener Disabled by default; true enables. If enabled, a job status
   *                              listener is registered. This can be useful for performance debug.
   * @param sparkKryoBufferSize Size of the object serialization buffer. Default setting is 4MB.
   * @param sparkMetricsListener A listener for recording metrics from Spark. Defaults to [[None]]
   * @return Returns a properly configured Spark Context.
   */
  def createSparkContext(name: String,
                         master: String,
                         sparkHome: String = null,
                         sparkJars: Seq[String] = Nil,
                         sparkEnvVars: Seq[(String, String)] = Nil,
                         sparkAddStatsListener: Boolean = false,
                         sparkKryoBufferSize: Int = 4,
                         sparkMetricsListener: Option[ADAMMetricsListener] = None,
                         loadSystemValues: Boolean = true,
                         sparkDriverPort: Option[Int] = None): SparkContext = {
    // TODO: Add enough spark arguments so that we don't need to load the system values (e.g. SPARK_MEM)
    val config: SparkConf = new SparkConf(loadSystemValues).setAppName("adam: " + name).setMaster(master)
    if (sparkHome != null)
      config.setSparkHome(sparkHome)
    if (sparkJars != Nil)
      config.setJars(sparkJars)
    if (sparkEnvVars != Nil)
      config.setExecutorEnv(sparkEnvVars)

    // Optionally set the spark driver port
    sparkDriverPort match {
      case Some(port) => config.set("spark.driver.port", port.toString)
      case None       =>
    }

    // Setup the Kryo settings
    config.setAll(Array(("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
      ("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator"),
      ("spark.kryoserializer.buffer.mb", sparkKryoBufferSize.toString),
      ("spark.kryo.referenceTracking", "true")))

    val sc = new SparkContext(config)

    if (sparkAddStatsListener) {
      sc.addSparkListener(new StatsReportListener)
    }

    sparkMetricsListener.foreach(sc.addSparkListener(_))

    sc
  }
}

class ADAMContext(sc: SparkContext) extends Serializable with Logging {

  private def adamBamDictionaryLoad(filePath: String): SequenceDictionary = {
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    adamBamDictionaryLoad(samHeader)
  }

  private def adamBamDictionaryLoad(samHeader: SAMFileHeader): SequenceDictionary = {
    SequenceDictionary(samHeader)

  }

  private def adamBamLoadReadGroups(samHeader: SAMFileHeader): RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }

  protected[rdd] def adamBamLoad(filePath: String): RDD[ADAMRecord] = {
    log.info("Reading legacy BAM file format %s to create RDD".format(filePath))

    // We need to separately read the header, so that we can inject the sequence dictionary
    // data into each individual ADAMRecord (see the argument to samRecordConverter.convert,
    // below).
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    val seqDict = adamBamDictionaryLoad(samHeader)
    val readGroups = adamBamLoadReadGroups(samHeader)

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    val samRecordConverter = new SAMRecordConverter
    records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups))
  }

  private def adamParquetLoad[T <% SpecificRecord: Manifest, U <: UnboundRecordFilter](filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None): RDD[T] = {
    log.info("Reading the ADAM file at %s to create RDD".format(filePath))
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])
    if (predicate.isDefined) {
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setUnboundRecordFilter(job, predicate.get)
    }
    if (projection.isDefined) {
      log.info("Using the specified projection schema")
      AvroParquetInputFormat.setRequestedProjection(job, projection.get)
    }
    val records = sc.newAPIHadoopFile(filePath,
      classOf[ParquetInputFormat[T]], classOf[Void], manifest[T].runtimeClass.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job)).map(p => p._2)
    if (predicate.isDefined) {
      // Strip the nulls that the predicate returns
      records.filter(p => p != null.asInstanceOf[T])
    } else {
      records
    }
  }

  /**
   * This method should create a new SequenceDictionary from any parquet file which contains
   * records that have the requisite reference{Name,Id,Length,Url} fields.
   *
   * (If the path is a BAM or SAM file, and the implicit type is an ADAMRecord, then it just defaults
   * to reading the SequenceDictionary out of the BAM header in the normal way.)
   *
   * @param filePath The path to the input data
   * @tparam T The type of records to return
   * @return A sequenceDictionary containing the names and indices of all the sequences to which the records
   *         in the corresponding file are aligned.
   */
  def adamDictionaryLoad[T <% SpecificRecord: Manifest](filePath: String): SequenceDictionary = {

    // This funkiness is required because (a) ADAMRecords require a different projection from any
    // other flattened schema, and (b) because the SequenceRecord.fromADAMRecord, below, is going
    // to be called through a flatMap rather than through a map tranformation on the underlying record RDD.
    val isADAMRecord = classOf[ADAMRecord].isAssignableFrom(manifest[T].runtimeClass)
    val isADAMContig = classOf[ADAMNucleotideContigFragment].isAssignableFrom(manifest[T].runtimeClass)

    val projection =
      if (isADAMRecord) {
        Projection(
          ADAMRecordField.contig,
          ADAMRecordField.mateContig,
          ADAMRecordField.readPaired,
          ADAMRecordField.firstOfPair,
          ADAMRecordField.readMapped,
          ADAMRecordField.mateMapped)
      } else if (isADAMContig) {
        Projection(ADAMNucleotideContigFragmentField.contig)
      } else {
        Projection(ADAMRecordField.contig)
      }

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam")) {
      if (isADAMRecord)
        adamBamDictionaryLoad(filePath)
      else
        throw new IllegalArgumentException("If you're reading a BAM/SAM file, the record type must be ADAMRecord")

    } else {
      val projected: RDD[T] = adamParquetLoad[T, UnboundRecordFilter](filePath, None, projection = Some(projection))

      val recs: RDD[SequenceRecord] =
        if (isADAMRecord) {
          projected.asInstanceOf[RDD[ADAMRecord]].distinct().flatMap(rec => SequenceRecord.fromADAMRecord(rec))
        } else if (isADAMContig) {
          projected.asInstanceOf[RDD[ADAMNucleotideContigFragment]].distinct().map(ctg => SequenceRecord.fromADAMContigFragment(ctg))
        } else {
          projected.distinct().map(SequenceRecord.fromSpecificRecord(_))
        }

      val dict = recs.aggregate(SequenceDictionary())(
        (dict: SequenceDictionary, rec: SequenceRecord) => dict + rec,
        (dict1: SequenceDictionary, dict2: SequenceDictionary) => dict1 ++ dict2)

      dict
    }
  }

  /**
   * This method will create a new RDD.
   * @param filePath The path to the input data
   * @param predicate An optional pushdown predicate to use when reading the data
   * @param projection An option projection schema to use when reading the data
   * @tparam T The type of records to return
   * @return An RDD with records of the specified type
   */
  def adamLoad[T <% SpecificRecord: Manifest, U <: ADAMPredicate[T]](filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None): RDD[T] = {

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam") && classOf[ADAMRecord].isAssignableFrom(manifest[T].runtimeClass)) {

      if (projection.isDefined) {
        log.warn("Projection is ignored when loading a BAM file")
      }
      val reads = adamBamLoad(filePath).asInstanceOf[RDD[T]]
      if (predicate.isDefined) {
        val predicateClass = predicate.get
        val filter = predicateClass.newInstance()
        filter(reads)
      } else {
        reads
      }
    } else {
      adamParquetLoad(filePath, predicate, projection)
    }
  }

  def adamSequenceLoad(filePath: String, fragmentLength: Long): RDD[ADAMNucleotideContigFragment] = {
    if (filePath.endsWith(".fasta") || filePath.endsWith(".fa")) {
      val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(filePath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text])

      val remapData = fastaData.map(kv => (kv._1.get.toLong, kv._2.toString))

      log.info("Converting FASTA to ADAM.")
      FastaConverter(remapData, fragmentLength)
    } else {
      adamParquetLoad(filePath)
    }
  }

  /**
   * Searches a path recursively, returning the names of all directories in the tree whose
   * name matches the given regex.
   *
   * @param path The path to begin the search at
   * @param regex A regular expression
   * @return A sequence of Path objects corresponding to the identified directories.
   */
  def findFiles(path: Path, regex: String): Seq[Path] = {
    if (regex == null) {
      Seq(path)
    } else {
      val statuses = FileSystem.get(sc.hadoopConfiguration).listStatus(path)
      val r = Pattern.compile(regex)
      val (matches, recurse) = statuses.filter(HadoopUtil.isDirectory).map(s => s.getPath).partition(p => r.matcher(p.getName).matches())
      matches.toSeq ++ recurse.flatMap(p => findFiles(p, regex))
    }
  }

  /**
   * Takes a sequence of Path objects (e.g. the return value of findFiles).  Treats each path as
   * corresponding to a ADAMRecord set -- loads each ADAMRecord set, converts each set to use the
   * same SequenceDictionary, and returns the union of the RDDs.
   *
   * (GenomeBridge is using this to load BAMs that have been split into multiple files per sample,
   * for example, one-BAM-per-chromosome.)
   *
   * @param paths The locations of the parquet files to load
   * @return a single RDD[ADAMRecord] that contains the union of the ADAMRecords in the argument paths.
   */
  def loadADAMFromPaths(paths: Seq[Path]): RDD[ADAMRecord] = {
    def loadADAMs(path: Path): (SequenceDictionary, RDD[ADAMRecord]) = {
      val dict = adamDictionaryLoad[ADAMRecord](path.toString)
      val rdd: RDD[ADAMRecord] = adamLoad(path.toString)
      (dict, rdd)
    }

    // Remapreferenceid code deleted since we don't remap sequence
    // dictionaries anymore.
    sc.union(paths.map(loadADAMs).map(v => v._2))
  }

}
