/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.converters.SAMRecordConverter
import edu.berkeley.cs.amplab.adam.models._
import org.apache.hadoop.fs.FileSystem
import edu.berkeley.cs.amplab.adam.projections.{ADAMRecordField, Projection}
import edu.berkeley.cs.amplab.adam.rdd.compare.CompareAdam
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import fi.tkk.ics.hadoop.bam.{SAMRecordWritable, AnySAMInputFormat}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil
import scala.Some
import scala.collection.JavaConversions._
import scala.collection.Map
import net.sf.samtools.SAMFileHeader
import java.util.regex.Pattern
import edu.berkeley.cs.amplab.adam.rich._
import edu.berkeley.cs.amplab.adam.rich.RichRDDReferenceRecords._

object AdamContext {
  // Add ADAM Spark context methods
  implicit def sparkContextToAdamContext(sc: SparkContext): AdamContext = new AdamContext(sc)

  // Add methods specific to ADAMRecord RDDs
  implicit def rddToAdamRecordRDD(rdd: RDD[ADAMRecord]) = new AdamRecordRDDFunctions(rdd)

  // Add methods specific to the ADAMPileup RDDs
  implicit def rddToAdamPileupRDD(rdd: RDD[ADAMPileup]) = new AdamPileupRDDFunctions(rdd)

  // Add methods specific to the ADAMRod RDDs
  implicit def rddToAdamRodRDD(rdd: RDD[ADAMRod]) = new AdamRodRDDFunctions(rdd)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToAdamRDD[T <% SpecificRecord : Manifest](rdd: RDD[T]) = new AdamRDDFunctions(rdd)

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
}

class AdamContext(sc: SparkContext) extends Serializable with Logging {

  private def adamBamDictionaryLoad(filePath: String): SequenceDictionary = {
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    adamBamDictionaryLoad(samHeader)
  }

  private def adamBamDictionaryLoad(samHeader : SAMFileHeader): SequenceDictionary = {
    SequenceDictionary.fromSAMHeader(samHeader)

  }

  private def adamBamLoadReadGroups(samHeader : SAMFileHeader) : RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }


  private def adamBamLoad(filePath: String): RDD[ADAMRecord] = {
    log.info("Reading legacy BAM file format %s to create RDD".format(filePath))

    // We need to separately read the header, so that we can inject the sequence dictionary
    // data into each individual ADAMRecord (see the argument to samRecordConverter.convert,
    // below).
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    val seqDict = adamBamDictionaryLoad(samHeader)
    val readGroups =  adamBamLoadReadGroups(samHeader)

    val job = new Job(sc.hadoopConfiguration)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    val samRecordConverter = new SAMRecordConverter
    records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups))
  }

  private def adamParquetLoad[T <% SpecificRecord : Manifest, U <: UnboundRecordFilter]
  (filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None): RDD[T] = {
    log.info("Reading the ADAM file at %s to create RDD".format(filePath))
    val job = new Job(sc.hadoopConfiguration)
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
      classOf[ParquetInputFormat[T]], classOf[Void], manifest[T].erasure.asInstanceOf[Class[T]],
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
  def adamDictionaryLoad[T <% SpecificRecord : Manifest](filePath: String): SequenceDictionary = {

    // This funkiness is required because (a) ADAMRecords require a different projection from any
    // other flattened schema, and (b) because the SequenceRecord.fromADAMRecord, below, is going
    // to be called through a flatMap rather than through a map tranformation on the underlying record RDD.
    val isAdamRecord = classOf[ADAMRecord].isAssignableFrom(manifest[T].erasure)

    val projection =
      if (isAdamRecord)
        Projection(
          ADAMRecordField.referenceId,
          ADAMRecordField.referenceName,
          ADAMRecordField.referenceLength,
          ADAMRecordField.referenceUrl,
          ADAMRecordField.mateReferenceId,
          ADAMRecordField.mateReference,
          ADAMRecordField.mateReferenceLength,
          ADAMRecordField.mateReferenceUrl,
          ADAMRecordField.readPaired,
          ADAMRecordField.firstOfPair,
          ADAMRecordField.readMapped,
          ADAMRecordField.mateMapped
        )
      else
        Projection(
          ADAMRecordField.referenceId,
          ADAMRecordField.referenceName,
          ADAMRecordField.referenceLength,
          ADAMRecordField.referenceUrl)

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam")) {
      if (isAdamRecord)
        adamBamDictionaryLoad(filePath)
      else
        throw new IllegalArgumentException("If you're reading a BAM/SAM file, the record type must be ADAMRecord")

    } else {
      val projected: RDD[T] = adamParquetLoad[T, UnboundRecordFilter](filePath, None, projection = Some(projection))

      val recs: RDD[SequenceRecord] =
        if (isAdamRecord)
          projected.asInstanceOf[RDD[ADAMRecord]].distinct().flatMap(rec => SequenceRecord.fromADAMRecord(rec))
        else
          projected.distinct().map(SequenceRecord.fromSpecificRecord(_))

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
  def adamLoad[T <% SpecificRecord : Manifest, U <: UnboundRecordFilter]
  (filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None): RDD[T] = {

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam") && classOf[ADAMRecord].isAssignableFrom(manifest[T].erasure)) {
      if (predicate.isDefined) {
        log.warn("Predicate is ignored when loading a BAM file")
      }
      if (projection.isDefined) {
        log.warn("Projection is ignored when loading a BAM file")
      }
      adamBamLoad(filePath).asInstanceOf[RDD[T]]
    } else {
      adamParquetLoad(filePath, predicate, projection)
    }
  }

  def adamCompareFiles(file1Path: String, file2Path: String,
                       predicateFactory: (Map[Int, Int]) => (SingleReadBucket, SingleReadBucket) => Boolean) = {
    CompareAdam.compareADAM(sc, file1Path, file2Path, predicateFactory)
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
      val (matches, recurse) = statuses.filter(s => s.isDir).map(s => s.getPath).partition(p => r.matcher(p.getName).matches())
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
  def loadAdamFromPaths(paths: Seq[Path]): RDD[ADAMRecord] = {
    def loadAdams(path: Path): (SequenceDictionary, RDD[ADAMRecord]) = {
      val dict = adamDictionaryLoad[ADAMRecord](path.toString)
      val rdd =adamLoad[ADAMRecord, UnboundRecordFilter](path.toString)
      (dict, rdd)
    }

    def remap(adams: Seq[(SequenceDictionary, RDD[ADAMRecord])]): Seq[RDD[ADAMRecord]] = {
      adams.headOption match {
        case None => Seq()
        case Some(head) =>
          head._2 +: adams.tail.map(v => {
            if (v._1.equals(head._1)) v._2
            else v._2.remapReferenceId(v._1.mapTo(head._1).toMap)(sc)
          })
      }
    }

    sc.union(remap(paths.map(loadAdams)))
  }
}



