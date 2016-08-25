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

import java.io.{ File, FileNotFoundException, InputStream }
import java.util.regex.Pattern

import com.google.common.collect.ImmutableList
import htsjdk.samtools.util.{ Interval, Locatable }
import htsjdk.samtools.{ SAMFileHeader, ValidationStringency }
import htsjdk.variant.vcf.VCFHeader
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.{ SpecificDatumReader, SpecificRecord, SpecificRecordBase }
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.parquet.avro.{ AvroParquetInputFormat, AvroReadSupport }
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.io._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.projections.{
  AlignmentRecordField,
  FeatureField,
  NucleotideContigFragmentField,
  Projection
}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.features._
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.{ AlignedReadRDD, AlignmentRecordRDD, UnalignedReadRDD }
import org.bdgenomics.adam.rdd.variation._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.{ ReferenceContigMap, ReferenceFile, TwoBitFile }
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.bdgenomics.utils.misc.{ HadoopUtil, Logging }
import org.seqdoop.hadoop_bam._
import org.seqdoop.hadoop_bam.util._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.reflect.ClassTag

// only used with indexedbamload
private case class LocatableReferenceRegion(rr: ReferenceRegion) extends Locatable {
  def getStart(): Int = rr.start.toInt + 1
  def getEnd(): Int = rr.end.toInt
  def getContig(): String = rr.referenceName
}

object ADAMContext {
  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext): ADAMContext = new ADAMContext(sc)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToADAMRDD[T](rdd: RDD[T])(implicit ev1: T => IndexedRecord, ev2: Manifest[T]): ConcreteADAMRDDFunctions[T] = new ConcreteADAMRDDFunctions(rdd)

  // Add implicits for the rich adam objects
  implicit def recordToRichRecord(record: AlignmentRecord): RichAlignmentRecord = new RichAlignmentRecord(record)

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

  //  implicit def charSequenceToString(cs: CharSequence): String = cs.toString

  //  implicit def charSequenceToList(cs: CharSequence): List[Char] = cs.toCharArray.toList

  implicit def mapToJavaMap[A, B](map: Map[A, B]): java.util.Map[A, B] = mapAsJavaMap(map)

  implicit def javaMapToMap[A, B](map: java.util.Map[A, B]): Map[A, B] = mapAsScalaMap(map).toMap

  implicit def iterableToJavaCollection[A](i: Iterable[A]): java.util.Collection[A] = asJavaCollection(i)

  implicit def setToJavaSet[A](set: Set[A]): java.util.Set[A] = setAsJavaSet(set)
}

import org.bdgenomics.adam.rdd.ADAMContext._

class ADAMContext(@transient val sc: SparkContext) extends Serializable with Logging {

  private[rdd] def loadBamDictionary(filePath: String): SequenceDictionary = {
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    loadBamDictionary(samHeader)
  }

  private[rdd] def loadBamDictionary(samHeader: SAMFileHeader): SequenceDictionary = {
    SequenceDictionary(samHeader)
  }

  private[rdd] def loadBamReadGroups(samHeader: SAMFileHeader): RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }

  private[rdd] def loadVcfMetadata(filePath: String): (SequenceDictionary, Seq[Sample]) = {
    def headerToMetadata(vcfHeader: VCFHeader): (SequenceDictionary, Seq[Sample]) = {
      val sd = SequenceDictionary.fromVCFHeader(vcfHeader)
      val samples = asScalaBuffer(vcfHeader.getGenotypeSamples)
        .map(s => {
          Sample.newBuilder()
            .setSampleId(s)
            .build()
        }).toSeq
      (sd, samples)
    }

    val vcfHeader = VCFHeaderReader.readHeaderFrom(WrapSeekable.openPath(sc.hadoopConfiguration,
      new Path(filePath)))
    headerToMetadata(vcfHeader)
  }

  private[rdd] def loadAvroSequences(filePath: String): SequenceDictionary = {
    val avroSd = loadAvro[Contig]("%s/_seqdict.avro".format(filePath),
      Contig.SCHEMA$)
    SequenceDictionary.fromAvro(avroSd)
  }

  private[rdd] def loadAvroSampleMetadata(filePath: String): Seq[Sample] = {
    loadAvro[Sample]("%s/_samples.avro".format(filePath),
      Sample.SCHEMA$)
  }

  private[rdd] def loadAvroReadGroupMetadata(filePath: String): RecordGroupDictionary = {
    val avroRgd = loadAvro[RecordGroupMetadata]("%s/_rgdict.avro".format(filePath),
      RecordGroupMetadata.SCHEMA$)

    // convert avro to record group dictionary
    new RecordGroupDictionary(avroRgd.map(RecordGroup.fromAvro))
  }

  /**
   * This method will create a new RDD.
   *
   * @param filePath The path to the input data
   * @param predicate An optional pushdown predicate to use when reading the data
   * @param projection An option projection schema to use when reading the data
   * @tparam T The type of records to return
   * @return An RDD with records of the specified type
   */
  def loadParquet[T](
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): RDD[T] = {
    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    log.info("Reading the ADAM file at %s to create RDD".format(filePath))
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])

    predicate.foreach { (pred) =>
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, pred)
    }

    if (projection.isDefined) {
      log.info("Using the specified projection schema")
      AvroParquetInputFormat.setRequestedProjection(job, projection.get)
    }

    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      manifest[T].runtimeClass.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job)
    )

    val instrumented = if (Metrics.isRecording) records.instrument() else records
    val mapped = instrumented.map(p => p._2)

    if (predicate.isDefined) {
      // Strip the nulls that the predicate returns
      mapped.filter(p => p != null.asInstanceOf[T])
    } else {
      mapped
    }
  }

  /**
   * This method should create a new SequenceDictionary from any parquet file which contains
   * records that have the requisite reference{Name,Id,Length,Url} fields.
   *
   * (If the path is a BAM or SAM file, and the implicit type is an Read, then it just defaults
   * to reading the SequenceDictionary out of the BAM header in the normal way.)
   *
   * @param filePath The path to the input data
   * @tparam T The type of records to return
   * @return A sequenceDictionary containing the names and indices of all the sequences to which the records
   *         in the corresponding file are aligned.
   */
  def loadDictionary[T](filePath: String)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): SequenceDictionary = {

    // This funkiness is required because (a) ADAMRecords require a different projection from any
    // other flattened schema, and (b) because the SequenceRecord.fromADAMRecord, below, is going
    // to be called through a flatMap rather than through a map tranformation on the underlying record RDD.
    val isADAMRecord = classOf[AlignmentRecord].isAssignableFrom(manifest[T].runtimeClass)
    val isADAMContig = classOf[NucleotideContigFragment].isAssignableFrom(manifest[T].runtimeClass)

    val projection =
      if (isADAMRecord) {
        Projection(
          AlignmentRecordField.contigName,
          AlignmentRecordField.mateContigName,
          AlignmentRecordField.readPaired,
          AlignmentRecordField.readInFragment,
          AlignmentRecordField.readMapped,
          AlignmentRecordField.mateMapped
        )
      } else if (isADAMContig) {
        Projection(NucleotideContigFragmentField.contig)
      } else {
        Projection(AlignmentRecordField.contigName)
      }

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam")) {
      if (isADAMRecord)
        loadBamDictionary(filePath)
      else
        throw new IllegalArgumentException("If you're reading a BAM/SAM file, the record type must be Read")

    } else {
      val projected: RDD[T] = loadParquet[T](filePath, None, projection = Some(projection))

      val recs: RDD[SequenceRecord] =
        if (isADAMContig) {
          projected.asInstanceOf[RDD[NucleotideContigFragment]].distinct().map(ctg => SequenceRecord.fromADAMContigFragment(ctg))
        } else {
          projected.distinct().map(SequenceRecord.fromSpecificRecord(_))
        }

      val dict = recs.aggregate(SequenceDictionary())(
        (dict: SequenceDictionary, rec: SequenceRecord) => dict + rec,
        (dict1: SequenceDictionary, dict2: SequenceDictionary) => dict1 ++ dict2
      )

      dict
    }
  }

  /**
   * Loads a SAM/BAM file.
   *
   * This reads the sequence and record group dictionaries from the SAM/BAM file
   * header. SAMRecords are read from the file and converted to the
   * AlignmentRecord schema.
   *
   * @param filePath Path to the file on disk.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @see loadAlignments
   */
  def loadBam(filePath: String,
              validationStringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = {
    val path = new Path(filePath)
    val fs =
      Option(
        FileSystem.get(path.toUri, sc.hadoopConfiguration)
      ).getOrElse(
          throw new FileNotFoundException(
            s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
          )
        )

    val bamFiles =
      Option(
        if (fs.isDirectory(path)) fs.listStatus(path) else fs.globStatus(path)
      ).getOrElse(
          throw new FileNotFoundException(
            s"Couldn't find any files matching ${path.toUri}"
          )
        )

    val (seqDict, readGroups) =
      bamFiles
        .map(fs => fs.getPath)
        .flatMap(fp => {
          try {
            // We need to separately read the header, so that we can inject the sequence dictionary
            // data into each individual Read (see the argument to samRecordConverter.convert,
            // below).
            sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, validationStringency.toString)
            val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)
            log.info("Loaded header from " + fp)
            val sd = loadBamDictionary(samHeader)
            val rg = loadBamReadGroups(samHeader)
            Some((sd, rg))
          } catch {
            case e: Throwable => {
              log.error(
                s"Loading failed for $fp:n${e.getMessage}\n\t${e.getStackTrace.take(25).map(_.toString).mkString("\n\t")}"
              )
              None
            }
          }
        }).reduce((kv1, kv2) => {
          (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2)
        })

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    if (Metrics.isRecording) records.instrument() else records
    val samRecordConverter = new SAMRecordConverter

    AlignedReadRDD(records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups)),
      seqDict,
      readGroups)
  }

  /**
   * Functions like loadBam, but uses bam index files to look at fewer blocks,
   * and only returns records within a specified ReferenceRegion. Bam index file required.
   *
   * @param filePath The path to the input data. Currently this path must correspond to
   *        a single Bam file. The bam index file associated needs to have the same name.
   * @param viewRegion The ReferenceRegion we are filtering on
   */
  def loadIndexedBam(filePath: String, viewRegion: ReferenceRegion): AlignmentRecordRDD = {
    loadIndexedBam(filePath, Iterable(viewRegion))
  }

  /**
   * Functions like loadBam, but uses bam index files to look at fewer blocks,
   * and only returns records within the specified ReferenceRegions. Bam index file required.
   *
   * @param filePath The path to the input data. Currently this path must correspond to
   *        a single Bam file. The bam index file associated needs to have the same name.
   * @param viewRegions Iterable of ReferenceRegions we are filtering on
   */
  def loadIndexedBam(filePath: String, viewRegions: Iterable[ReferenceRegion])(implicit s: DummyImplicit): AlignmentRecordRDD = {
    val path = new Path(filePath)
    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
    assert(!fs.isDirectory(path))
    val bamfile: Array[FileStatus] = fs.globStatus(path)
    require(bamfile.size == 1)
    val (seqDict, readGroups) = bamfile
      .map(fs => fs.getPath)
      .flatMap(fp => {
        try {
          // We need to separately read the header, so that we can inject the sequence dictionary
          // data into each individual Read (see the argument to samRecordConverter.convert,
          // below).
          val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)

          log.info("Loaded header from " + fp)
          val sd = loadBamDictionary(samHeader)
          val rg = loadBamReadGroups(samHeader)
          Some((sd, rg))
        } catch {
          case _: Throwable => {
            log.error("Loading failed for " + fp)
            None
          }
        }
      }).reduce((kv1, kv2) => {
        (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2)
      })

    val samDict = SAMHeaderReader.readSAMHeaderFrom(path, sc.hadoopConfiguration).getSequenceDictionary

    val job = HadoopUtil.newJob(sc)
    val conf = ContextUtil.getConfiguration(job)
    BAMInputFormat.setIntervals(conf, viewRegions.toList.map(r => LocatableReferenceRegion(r)))

    val records = sc.newAPIHadoopFile(filePath, classOf[BAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], conf)
    if (Metrics.isRecording) records.instrument() else records
    val samRecordConverter = new SAMRecordConverter
    AlignedReadRDD(records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups)),
      seqDict,
      readGroups)
  }

  /**
   * Loads Avro data from a Hadoop File System.
   *
   * This method uses the SparkContext wrapped by this class to identify our
   * underlying file system. We then use the underlying FileSystem imp'l to
   * open the Avro file, and we read the Avro files into a Seq.
   *
   * Frustratingly enough, although all records generated by the Avro IDL
   * compiler have a static SCHEMA$ field, this field does not belong to
   * the SpecificRecordBase abstract class, or the SpecificRecord interface.
   * As such, we must force the user to pass in the schema.
   *
   * @tparam T The type of the specific record we are loading.
   * @param filename Path to Vf file from.
   * @param schema Schema of records we are loading.
   * @return Returns a Seq containing the avro records.
   */
  private def loadAvro[T <: SpecificRecordBase](filename: String,
                                                schema: Schema)(
                                                  implicit tTag: ClassTag[T]): Seq[T] = {

    // get our current file system
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // get an input stream
    val is = fs.open(new Path(filename))
      .asInstanceOf[InputStream]

    // set up avro for reading
    val dr = new SpecificDatumReader[T](schema)
    val fr = new DataFileStream[T](is, dr)

    // get iterator and create an empty list
    val iter = fr.iterator
    var list = List.empty[T]

    // !!!!!
    // important implementation note:
    // !!!!!
    //
    // in theory, we should be able to call iter.toSeq to get a Seq of the
    // specific records we are reading. this would allow us to avoid needing
    // to manually pop things into a list.
    //
    // however! this causes odd problems that seem to be related to some sort of
    // lazy execution inside of scala. specifically, if you go
    // iter.toSeq.map(fn) in scala, this seems to be compiled into a lazy data
    // structure where the map call is only executed when the Seq itself is
    // actually accessed (e.g., via seq.apply(i), seq.head, etc.). typically,
    // this would be OK, but if the Seq[T] goes into a spark closure, the closure
    // cleaner will fail with a NotSerializableException, since SpecificRecord's
    // are not java serializable. specifically, we see this happen when using
    // this function to load RecordGroupMetadata when creating a
    // RecordGroupDictionary.
    //
    // good news is, you can work around this by explicitly walking the iterator
    // and building a collection, which is what we do here. this would not be
    // efficient if we were loading a large amount of avro data (since we're
    // loading all the data into memory), but currently, we are just using this
    // code for building sequence/record group dictionaries, which are fairly
    // small (seq dict is O(30) entries, rgd is O(20n) entries, where n is the
    // number of samples).
    while (iter.hasNext) {
      list = iter.next :: list
    }

    // close file
    fr.close()
    is.close()

    // reverse list and return as seq
    list.reverse
      .toSeq
  }

  /**
   * Loads alignment data from a Parquet file.
   *
   * @param filePath The path of the file to load.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional schema designating the fields to project.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @note The sequence dictionary is read from an avro file stored at
   *   filePath/_seqdict.avro and the record group dictionary is read from an
   *   avro file stored at filePath/_rgdict.avro. These files are pure avro,
   *   not Parquet.
   * @see loadAlignments
   */
  def loadParquetAlignments(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): AlignmentRecordRDD = {

    // load from disk
    val rdd = loadParquet[AlignmentRecord](filePath, predicate, projection)

    // convert avro to sequence dictionary
    val sd = loadAvroSequences(filePath)

    // convert avro to sequence dictionary
    val rgd = loadAvroReadGroupMetadata(filePath)

    AlignedReadRDD(rdd, sd, rgd)
  }

  def loadInterleavedFastq(
    filePath: String): AlignmentRecordRDD = {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[InterleavedFastqInputFormat],
      classOf[Void],
      classOf[Text],
      ContextUtil.getConfiguration(job)
    )
    if (Metrics.isRecording) records.instrument() else records

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    UnalignedReadRDD.fromRdd(records.flatMap(fastqRecordConverter.convertPair))
  }

  def loadFastq(
    filePath1: String,
    filePath2Opt: Option[String],
    recordGroupOpt: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = {
    filePath2Opt match {
      case Some(filePath2) => loadPairedFastq(filePath1, filePath2, recordGroupOpt, stringency)
      case None            => loadUnpairedFastq(filePath1, recordGroupOpt, stringency = stringency)
    }
  }

  def loadPairedFastq(
    filePath1: String,
    filePath2: String,
    recordGroupOpt: Option[String],
    stringency: ValidationStringency): AlignmentRecordRDD = {
    val reads1 = loadUnpairedFastq(filePath1, recordGroupOpt, setFirstOfPair = true, stringency = stringency)
    val reads2 = loadUnpairedFastq(filePath2, recordGroupOpt, setSecondOfPair = true, stringency = stringency)

    stringency match {
      case ValidationStringency.STRICT | ValidationStringency.LENIENT =>
        val count1 = reads1.rdd.cache.count
        val count2 = reads2.rdd.cache.count

        if (count1 != count2) {
          val msg = s"Fastq 1 ($filePath1) has $count1 reads, fastq 2 ($filePath2) has $count2 reads"
          if (stringency == ValidationStringency.STRICT)
            throw new IllegalArgumentException(msg)
          else {
            // ValidationStringency.LENIENT
            logError(msg)
          }
        }
      case ValidationStringency.SILENT =>
    }

    UnalignedReadRDD.fromRdd(reads1.rdd ++ reads2.rdd)
  }

  def loadUnpairedFastq(
    filePath: String,
    recordGroupOpt: Option[String] = None,
    setFirstOfPair: Boolean = false,
    setSecondOfPair: Boolean = false,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text],
      ContextUtil.getConfiguration(job)
    )
    if (Metrics.isRecording) records.instrument() else records

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    UnalignedReadRDD.fromRdd(records.map(
      fastqRecordConverter.convertRead(
        _,
        recordGroupOpt.map(recordGroup =>
          if (recordGroup.isEmpty)
            filePath.substring(filePath.lastIndexOf("/") + 1)
          else
            recordGroup),
        setFirstOfPair,
        setSecondOfPair,
        stringency
      )
    ))
  }

  private def readVcfRecords(filePath: String, viewRegions: Option[Iterable[ReferenceRegion]]): RDD[(LongWritable, VariantContextWritable)] = {
    // load vcf data
    val job = HadoopUtil.newJob(sc)
    job.getConfiguration().setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName(),
      classOf[BGZFEnhancedGzipCodec].getCanonicalName())

    val conf = ContextUtil.getConfiguration(job)
    if (viewRegions.isDefined) {
      val intervals: Iterable[Interval] =
        viewRegions.get.map(r => {
          new Interval(r.referenceName, r.start.toInt, r.end.toInt)
        })
      VCFInputFormat.setIntervals(conf, ImmutableList.copyOf[Interval](intervals.asJava))
    }

    sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      conf
    )
  }

  /**
   * Loads a VCF file into an RDD.
   *
   * @param filePath The file to load.
   * @return Returns a VariantContextRDD.
   */
  def loadVcf(filePath: String): VariantContextRDD = {

    // load records from VCF
    val records = readVcfRecords(filePath, None)

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples) = loadVcfMetadata(filePath)

    val vcc = new VariantContextConverter(Some(sd))

    VariantContextRDD(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples)
  }

  /**
   * Loads a VCF file indexed by a tabix (tbi) file into an RDD.
   *
   * @param filePath The file to load.
   * @param viewRegion ReferenceRegions we are filtering on.
   * @return Returns a VariantContextRDD.
   */
  def loadIndexedVcf(filePath: String,
                     viewRegion: ReferenceRegion): VariantContextRDD =
    loadIndexedVcf(filePath, Iterable(viewRegion))

  /**
   * Loads a VCF file indexed by a tabix (tbi) file into an RDD.
   *
   * @param filePath The file to load.
   * @param viewRegions Iterator of ReferenceRegions we are filtering on.
   * @return Returns a VariantContextRDD.
   */
  def loadIndexedVcf(filePath: String,
                     viewRegions: Iterable[ReferenceRegion])(implicit s: DummyImplicit): VariantContextRDD = {

    // load records from VCF
    val records = readVcfRecords(filePath, Some(viewRegions))

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples) = loadVcfMetadata(filePath)

    val vcc = new VariantContextConverter(Some(sd))

    VariantContextRDD(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples)
  }

  def loadParquetGenotypes(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): GenotypeRDD = {
    val rdd = loadParquet[Genotype](filePath, predicate, projection)

    // load sequence info
    val sd = loadAvroSequences(filePath)

    // load avro record group dictionary and convert to samples
    val samples = loadAvroSampleMetadata(filePath)

    GenotypeRDD(rdd, sd, samples)
  }

  def loadParquetVariants(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): VariantRDD = {
    val rdd = loadParquet[Variant](filePath, predicate, projection)
    val sd = loadAvroSequences(filePath)

    VariantRDD(rdd, sd)
  }

  def loadFasta(
    filePath: String,
    fragmentLength: Long): NucleotideContigFragmentRDD = {
    val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(
      filePath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]
    )
    if (Metrics.isRecording) fastaData.instrument() else fastaData

    val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

    // convert rdd and cache
    val fragmentRdd = FastaConverter(remapData, fragmentLength)
      .cache()

    NucleotideContigFragmentRDD(fragmentRdd)
  }

  def loadInterleavedFastqAsFragments(
    filePath: String): FragmentRDD = {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[InterleavedFastqInputFormat],
      classOf[Void],
      classOf[Text],
      ContextUtil.getConfiguration(job)
    )
    if (Metrics.isRecording) records.instrument() else records

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    FragmentRDD.fromRdd(records.map(fastqRecordConverter.convertFragment))
  }

  /**
   * Loads Parquet file of Features to a CoverageRDD.
   * Coverage is stored in the score attribute of Feature.
   * @param filePath File path to load coverage from
   * @return CoverageRDD containing an RDD of Coverage
   */
  def loadCoverage(filePath: String): CoverageRDD = {
    val proj = Projection(FeatureField.contigName, FeatureField.start, FeatureField.end, FeatureField.score)
    loadFeatures(filePath, projection = Some(proj)).toCoverage
  }

  def loadGff3(filePath: String, minPartitions: Option[Int] = None): FeatureRDD = {
    val records = sc.textFile(filePath, minPartitions.getOrElse(sc.defaultParallelism)).flatMap(new GFF3Parser().parse)
    if (Metrics.isRecording) records.instrument() else records
    FeatureRDD(records)
  }

  def loadGtf(filePath: String, minPartitions: Option[Int] = None): FeatureRDD = {
    val records = sc.textFile(filePath, minPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new GTFParser().parse)
    if (Metrics.isRecording) records.instrument() else records
    FeatureRDD(records)
  }

  def loadBed(filePath: String, minPartitions: Option[Int] = None): FeatureRDD = {
    val records = sc.textFile(filePath, minPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new BEDParser().parse)
    if (Metrics.isRecording) records.instrument() else records
    FeatureRDD(records)
  }

  def loadNarrowPeak(filePath: String, minPartitions: Option[Int] = None): FeatureRDD = {
    val records = sc.textFile(filePath, minPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new NarrowPeakParser().parse)
    if (Metrics.isRecording) records.instrument() else records
    FeatureRDD(records)
  }

  def loadIntervalList(filePath: String, minPartitions: Option[Int] = None): FeatureRDD = {
    val parsedLines = sc.textFile(filePath, minPartitions.getOrElse(sc.defaultParallelism))
      .map(new IntervalListParser().parse)
    val (seqDict, records) = (SequenceDictionary(parsedLines.flatMap(_._1).collect(): _*), parsedLines.flatMap(_._2))
    val seqDictMap = seqDict.records.map(sr => sr.name -> sr).toMap
    val recordsWithContigs = for {
      record <- records
      seqRecord <- seqDictMap.get(record.getContigName)
    } yield Feature.newBuilder(record)
      .setContigName(seqRecord.name)
      .build()

    if (Metrics.isRecording) recordsWithContigs.instrument() else recordsWithContigs
    FeatureRDD(recordsWithContigs, seqDict)
  }

  def loadParquetFeatures(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): FeatureRDD = {
    val sd = loadAvroSequences(filePath)
    val rdd = loadParquet[Feature](filePath, predicate, projection)
    FeatureRDD(rdd, sd)
  }

  def loadParquetContigFragments(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): NucleotideContigFragmentRDD = {
    val sd = loadAvroSequences(filePath)
    val rdd = loadParquet[NucleotideContigFragment](filePath, predicate, projection)
    NucleotideContigFragmentRDD(rdd, sd)
  }

  def loadParquetFragments(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): FragmentRDD = {

    // convert avro to sequence dictionary
    val sd = loadAvroSequences(filePath)

    // convert avro to sequence dictionary
    val rgd = loadAvroReadGroupMetadata(filePath)

    // load fragment data from parquet
    val rdd = loadParquet[Fragment](filePath, predicate, projection)

    FragmentRDD(rdd, sd, rgd)
  }

  def loadVcfAnnotations(
    filePath: String): DatabaseVariantAnnotationRDD = {
    loadVcf(filePath).toDatabaseVariantAnnotationRDD
  }

  def loadParquetVariantAnnotations(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): DatabaseVariantAnnotationRDD = {
    val sd = loadAvroSequences(filePath)
    val rdd = loadParquet[DatabaseVariantAnnotation](filePath, predicate, projection)
    DatabaseVariantAnnotationRDD(rdd, sd)
  }

  def loadVariantAnnotations(
    filePath: String,
    projection: Option[Schema] = None): DatabaseVariantAnnotationRDD = {
    if (filePath.endsWith(".vcf")) {
      log.info(s"Loading $filePath as VCF, and converting to variant annotations. Projection is ignored.")
      loadVcfAnnotations(filePath)
    } else {
      log.info(s"Loading $filePath as Parquet containing DatabaseVariantAnnotations.")
      loadParquetVariantAnnotations(filePath, None, projection)
    }
  }

  def loadFeatures(filePath: String,
                   projection: Option[Schema],
                   minPartitions: Int): FeatureRDD = {
    loadFeatures(filePath, projection, Some(minPartitions))
  }

  def loadFeatures(filePath: String,
                   projection: Option[Schema] = None,
                   minPartitions: Option[Int] = None): FeatureRDD = {

    if (filePath.endsWith(".bed")) {
      log.info(s"Loading $filePath as BED and converting to features. Projection is ignored.")
      loadBed(filePath, minPartitions)
    } else if (filePath.endsWith(".gtf") ||
      filePath.endsWith(".gff")) {
      log.info(s"Loading $filePath as GTF/GFF and converting to features. Projection is ignored.")
      loadGtf(filePath, minPartitions)
    } else if (filePath.endsWith(".narrowPeak") ||
      filePath.endsWith(".narrowpeak")) {
      log.info(s"Loading $filePath as NarrowPeak and converting to features. Projection is ignored.")
      loadNarrowPeak(filePath, minPartitions)
    } else if (filePath.endsWith(".interval_list")) {
      log.info(s"Loading $filePath as IntervalList and converting to features. Projection is ignored.")
      loadIntervalList(filePath, minPartitions)
    } else {
      log.info(s"Loading $filePath as Parquet containing Features.")
      loadParquetFeatures(filePath, None, projection)
    }
  }

  def loadGenes(
    filePath: String,
    projection: Option[Schema] = None): GeneRDD = {
    loadFeatures(filePath, projection).toGenes()
  }

  def loadReferenceFile(filePath: String, fragmentLength: Long): ReferenceFile = {
    if (filePath.endsWith(".2bit")) {
      //TODO(ryan): S3ByteAccess
      new TwoBitFile(new LocalFileByteAccess(new File(filePath)))
    } else {
      ReferenceContigMap(loadSequences(filePath, fragmentLength = fragmentLength).rdd)
    }
  }

  def loadSequences(
    filePath: String,
    projection: Option[Schema] = None,
    fragmentLength: Long = 10000): NucleotideContigFragmentRDD = {
    if (filePath.endsWith(".fa") ||
      filePath.endsWith(".fasta") ||
      filePath.endsWith(".fa.gz") ||
      filePath.endsWith(".fasta.gz")) {
      log.info(s"Loading $filePath as FASTA and converting to NucleotideContigFragment. Projection is ignored.")
      loadFasta(
        filePath,
        fragmentLength
      )
    } else {
      log.info(s"Loading $filePath as Parquet containing NucleotideContigFragments.")
      loadParquetContigFragments(filePath, None, projection)
    }
  }

  def loadGenotypes(
    filePath: String,
    projection: Option[Schema] = None): GenotypeRDD = {
    if (filePath.endsWith(".vcf")) {
      log.info(s"Loading $filePath as VCF, and converting to Genotypes. Projection is ignored.")
      loadVcf(filePath).toGenotypeRDD
    } else {
      log.info(s"Loading $filePath as Parquet containing Genotypes. Sequence dictionary for translation is ignored.")
      loadParquetGenotypes(filePath, None, projection)
    }
  }

  def loadVariants(
    filePath: String,
    projection: Option[Schema] = None): VariantRDD = {
    if (filePath.endsWith(".vcf")) {
      log.info(s"Loading $filePath as VCF, and converting to Variants. Projection is ignored.")
      loadVcf(filePath).toVariantRDD
    } else {
      log.info(s"Loading $filePath as Parquet containing Variants. Sequence dictionary for translation is ignored.")
      loadParquetVariants(filePath, None, projection)
    }
  }

  /**
   * Loads alignments from a given path, and infers the input type.
   *
   * This method can load:
   *
   * * AlignmentRecords via Parquet (default)
   * * SAM/BAM (.sam, .bam)
   * * FASTQ (interleaved, single end, paired end) (.ifq, .fq/.fastq)
   * * FASTA (.fa, .fasta)
   * * NucleotideContigFragments via Parquet (.contig.adam)
   *
   * As hinted above, the input type is inferred from the file path extension.
   *
   * @param filePath Path to load data from.
   * @param projection The fields to project; ignored if not Parquet.
   * @param filePath2Opt The path to load a second end of FASTQ data from.
   *  Ignored if not FASTQ.
   * @param recordGroupOpt Optional record group name to set if loading FASTQ.
   * @param stringency Validation stringency used on FASTQ import/merging.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @see loadBam
   * @see loadParquetAlignments
   * @see loadInterleavedFastq
   * @see loadFastq
   * @see loadFasta
   */
  def loadAlignments(
    filePath: String,
    projection: Option[Schema] = None,
    filePath2Opt: Option[String] = None,
    recordGroupOpt: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = LoadAlignmentRecords.time {

    if (filePath.endsWith(".sam") ||
      filePath.endsWith(".bam")) {
      log.info(s"Loading $filePath as SAM/BAM and converting to AlignmentRecords. Projection is ignored.")
      loadBam(filePath, stringency)
    } else if (filePath.endsWith(".ifq")) {
      log.info(s"Loading $filePath as interleaved FASTQ and converting to AlignmentRecords. Projection is ignored.")
      loadInterleavedFastq(filePath)
    } else if (filePath.endsWith(".fq") ||
      filePath.endsWith(".fastq")) {
      log.info(s"Loading $filePath as unpaired FASTQ and converting to AlignmentRecords. Projection is ignored.")
      loadFastq(filePath, filePath2Opt, recordGroupOpt, stringency)
    } else if (filePath.endsWith(".fa") ||
      filePath.endsWith(".fasta")) {
      log.info(s"Loading $filePath as FASTA and converting to AlignmentRecords. Projection is ignored.")
      UnalignedReadRDD(loadFasta(filePath, fragmentLength = 10000).toReads,
        RecordGroupDictionary.empty)
    } else if (filePath.endsWith("contig.adam")) {
      log.info(s"Loading $filePath as Parquet of NucleotideContigFragment and converting to AlignmentRecords. Projection is ignored.")
      UnalignedReadRDD(loadParquetContigFragments(filePath).toReads, RecordGroupDictionary.empty)
    } else {
      log.info(s"Loading $filePath as Parquet of AlignmentRecords.")
      loadParquetAlignments(filePath, None, projection)
    }
  }

  def loadFragments(filePath: String): FragmentRDD = LoadFragments.time {
    if (filePath.endsWith(".sam") ||
      filePath.endsWith(".bam")) {
      log.info(s"Loading $filePath as SAM/BAM and converting to Fragments.")
      loadBam(filePath).toFragments
    } else if (filePath.endsWith(".reads.adam")) {
      log.info(s"Loading $filePath as ADAM AlignmentRecords and converting to Fragments.")
      loadAlignments(filePath).toFragments
    } else if (filePath.endsWith(".ifq")) {
      log.info("Loading interleaved FASTQ " + filePath + " and converting to Fragments.")
      loadInterleavedFastqAsFragments(filePath)
    } else {
      loadParquetFragments(filePath)
    }
  }

  /**
   * Takes a sequence of Path objects and loads alignments using that path.
   *
   * This infers the type of each path, and thus can be used to load a mixture
   * of different files from disk. I.e., if you want to load 2 BAM files and
   * 3 Parquet files, this is the method you are looking for!
   *
   * The RDDs obtained from loading each file are simply unioned together,
   * while the record group dictionaries are naively merged. The sequence
   * dictionaries are merged in a way that dedupes the sequence records in
   * each dictionary.
   *
   * @param paths The locations of the files to load.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @see loadAlignments
   */
  def loadAlignmentsFromPaths(paths: Seq[Path]): AlignmentRecordRDD = {

    val alignmentData = paths.map(p => loadAlignments(p.toString))

    val rdd = sc.union(alignmentData.map(_.rdd))
    val sd = alignmentData.map(_.sequences).reduce(_ ++ _)
    val rgd = alignmentData.map(_.recordGroups).reduce(_ ++ _)

    AlignedReadRDD(rdd, sd, rgd)
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
}
