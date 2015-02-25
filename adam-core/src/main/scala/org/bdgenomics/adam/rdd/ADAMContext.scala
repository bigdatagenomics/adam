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

import java.util.regex.Pattern
import htsjdk.samtools.SAMFileHeader
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.bdgenomics.adam.converters._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.io._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.predicates.ADAMPredicate
import org.bdgenomics.adam.projections.{ AlignmentRecordField, NucleotideContigFragmentField, Projection }
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDDFunctions
import org.bdgenomics.adam.rdd.features._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions
import org.bdgenomics.adam.rdd.variation._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.instrumentation.Metrics
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam._
import parquet.avro.{ AvroParquetInputFormat, AvroReadSupport }
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil
import scala.collection.JavaConversions._
import scala.collection.Map
import scala.reflect.ClassTag

object ADAMContext {
  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext): ADAMContext = new ADAMContext(sc)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToADAMRDD[T](rdd: RDD[T])(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): ADAMRDDFunctions[T] = new ADAMRDDFunctions(rdd)

  // Add methods specific to Read RDDs
  implicit def rddToADAMRecordRDD(rdd: RDD[AlignmentRecord]) = new AlignmentRecordRDDFunctions(rdd)

  // Add methods specific to the ADAMNucleotideContig RDDs
  implicit def rddToContigFragmentRDD(rdd: RDD[NucleotideContigFragment]) = new NucleotideContigFragmentRDDFunctions(rdd)

  // implicit conversions for variant related rdds
  implicit def rddToVariantContextRDD(rdd: RDD[VariantContext]) = new VariantContextRDDFunctions(rdd)
  implicit def rddToADAMGenotypeRDD(rdd: RDD[Genotype]) = new GenotypeRDDFunctions(rdd)

  // add gene feature rdd functions
  implicit def convertBaseFeatureRDDToGeneFeatureRDD(rdd: RDD[Feature]) = new GeneFeatureRDDFunctions(rdd)

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

import ADAMContext._

class ADAMContext(val sc: SparkContext) extends Serializable with Logging {

  private[rdd] def adamBamDictionaryLoad(filePath: String): SequenceDictionary = {
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    adamBamDictionaryLoad(samHeader)
  }

  private[rdd] def adamBamDictionaryLoad(samHeader: SAMFileHeader): SequenceDictionary = {
    SequenceDictionary(samHeader)
  }

  private[rdd] def adamBamLoadReadGroups(samHeader: SAMFileHeader): RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }

  /**
   * This method will create a new RDD.
   * @param filePath The path to the input data
   * @param predicate An optional pushdown predicate to use when reading the data
   * @param projection An option projection schema to use when reading the data
   * @tparam T The type of records to return
   * @return An RDD with records of the specified type
   */
  def adamLoad[T, U <: UnboundRecordFilter](filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): RDD[T] = {
    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    if (!filePath.endsWith(".adam")) {
      throw new IllegalArgumentException(
        "Expected '.adam' extension on file being loaded in ADAM format: %s".format(filePath)
      )
    }

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
  def adamDictionaryLoad[T](filePath: String)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): SequenceDictionary = {

    // This funkiness is required because (a) ADAMRecords require a different projection from any
    // other flattened schema, and (b) because the SequenceRecord.fromADAMRecord, below, is going
    // to be called through a flatMap rather than through a map tranformation on the underlying record RDD.
    val isADAMRecord = classOf[AlignmentRecord].isAssignableFrom(manifest[T].runtimeClass)
    val isADAMContig = classOf[NucleotideContigFragment].isAssignableFrom(manifest[T].runtimeClass)

    val projection =
      if (isADAMRecord) {
        Projection(
          AlignmentRecordField.contig,
          AlignmentRecordField.mateContig,
          AlignmentRecordField.readPaired,
          AlignmentRecordField.firstOfPair,
          AlignmentRecordField.readMapped,
          AlignmentRecordField.mateMapped)
      } else if (isADAMContig) {
        Projection(NucleotideContigFragmentField.contig)
      } else {
        Projection(AlignmentRecordField.contig)
      }

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam")) {
      if (isADAMRecord)
        adamBamDictionaryLoad(filePath)
      else
        throw new IllegalArgumentException("If you're reading a BAM/SAM file, the record type must be Read")

    } else {
      val projected: RDD[T] = adamLoad[T, UnboundRecordFilter](filePath, None, projection = Some(projection))

      val recs: RDD[SequenceRecord] =
        if (isADAMRecord) {
          projected.asInstanceOf[RDD[AlignmentRecord]].distinct().flatMap(rec => SequenceRecord.fromADAMRecord(rec))
        } else if (isADAMContig) {
          projected.asInstanceOf[RDD[NucleotideContigFragment]].distinct().map(ctg => SequenceRecord.fromADAMContigFragment(ctg))
        } else {
          projected.distinct().map(SequenceRecord.fromSpecificRecord(_))
        }

      val dict = recs.aggregate(SequenceDictionary())(
        (dict: SequenceDictionary, rec: SequenceRecord) => dict + rec,
        (dict1: SequenceDictionary, dict2: SequenceDictionary) => dict1 ++ dict2)

      dict
    }
  }

  def applyPredicate[T, U <: ADAMPredicate[T]](reads: RDD[T], predicateOpt: Option[Class[U]])(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): RDD[T] =
    predicateOpt.map(_.newInstance()(reads)).getOrElse(reads)

  private[rdd] def adamBamLoad(filePath: String): RDD[AlignmentRecord] = BAMLoad.time {
    log.info("Reading legacy BAM file format %s to create RDD".format(filePath))

    // We need to separately read the header, so that we can inject the sequence dictionary
    // data into each individual Read (see the argument to samRecordConverter.convert,
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

  private def maybeLoadBam[U <: ADAMPredicate[AlignmentRecord]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None): Option[RDD[AlignmentRecord]] = {

    if (filePath.endsWith(".bam") || filePath.endsWith(".sam")) {

      if (projection.isDefined) {
        log.warn("Projection is ignored when loading a BAM file")
      }

      val reads = adamBamLoad(filePath)

      Some(applyPredicate(reads, predicate))
    } else
      None
  }

  private def maybeLoadFastq[U <: ADAMPredicate[AlignmentRecord]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None): Option[RDD[AlignmentRecord]] = {

    if (filePath.endsWith(".ifq")) {

      log.info("Reading interleaved FASTQ file format %s to create RDD".format(filePath))
      if (projection.isDefined) {
        log.warn("Projection is ignored when loading an interleaved FASTQ file")
      }

      val job = HadoopUtil.newJob(sc)
      val records = sc.newAPIHadoopFile(
        filePath,
        classOf[InterleavedFastqInputFormat],
        classOf[Void],
        classOf[Text],
        ContextUtil.getConfiguration(job)
      )

      // convert records
      val fastqRecordConverter = new FastqRecordConverter
      Some(applyPredicate(records.flatMap(fastqRecordConverter.convertPair), predicate))
    } else if (filePath.endsWith(".fastq") || filePath.endsWith(".fq")) {

      log.info("Reading unpaired FASTQ file format %s to create RDD".format(filePath))
      if (projection.isDefined) {
        log.warn("Projection is ignored when loading a FASTQ file")
      }

      val job = HadoopUtil.newJob(sc)
      val records = sc.newAPIHadoopFile(
        filePath,
        classOf[SingleFastqInputFormat],
        classOf[Void],
        classOf[Text],
        ContextUtil.getConfiguration(job)
      )

      // convert records
      val fastqRecordConverter = new FastqRecordConverter
      Some(applyPredicate(records.map(fastqRecordConverter.convertRead), predicate))
    } else
      None
  }

  private def maybeLoadVcf(filePath: String, sd: Option[SequenceDictionary]): Option[RDD[VariantContext]] = {
    if (filePath.endsWith(".vcf")) {
      log.info("Reading VCF file from %s".format(filePath))
      val job = HadoopUtil.newJob(sc)
      val vcc = new VariantContextConverter(sd)
      val records = sc.newAPIHadoopFile(
        filePath,
        classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
        ContextUtil.getConfiguration(job))

      Some(records.flatMap(p => vcc.convert(p._2.get)))
    } else
      None
  }

  private def maybeLoadFasta[U <: ADAMPredicate[NucleotideContigFragment]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None,
    fragmentLength: Long): Option[RDD[NucleotideContigFragment]] = {
    if (filePath.endsWith(".fasta") || filePath.endsWith(".fa")) {
      val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(filePath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text])

      val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

      log.info("Converting FASTA to ADAM.")
      Some(FastaConverter(remapData, fragmentLength))
    } else {
      None
    }
  }

  private def maybeLoadGTF(filePath: String): Option[RDD[Feature]] = {
    if (filePath.endsWith(".gtf") || filePath.endsWith(".gff")) {
      Some(sc.textFile(filePath).flatMap(new GTFParser().parse))
    } else {
      None
    }
  }

  private def maybeLoadBED(filePath: String): Option[RDD[Feature]] = {
    if (filePath.endsWith(".bed")) {
      Some(sc.textFile(filePath).flatMap(new BEDParser().parse))
    } else {
      None
    }
  }

  private def maybeLoadNarrowPeak(filePath: String): Option[RDD[Feature]] = {
    if (filePath.toLowerCase.endsWith(".narrowpeak")) {
      Some(sc.textFile(filePath).flatMap(new NarrowPeakParser().parse))
    } else {
      None
    }
  }

  private def maybeLoadVcfAnnotations[U <: ADAMPredicate[DatabaseVariantAnnotation]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary]): Option[RDD[DatabaseVariantAnnotation]] = {
    if (filePath.endsWith(".vcf")) {
      log.info("Reading VCF file from %s".format(filePath))
      if (projection.isDefined) {
        log.warn("Projection is ignored when loading a VCF file")
      }

      val job = HadoopUtil.newJob(sc)
      val vcc = new VariantContextConverter(sd)
      val records = sc.newAPIHadoopFile(
        filePath,
        classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
        ContextUtil.getConfiguration(job))

      Some(applyPredicate(records.map(p => vcc.convertToAnnotation(p._2.get)), predicate))
    } else
      None
  }

  def loadVariantAnnotations[U <: ADAMPredicate[DatabaseVariantAnnotation]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary] = None): RDD[DatabaseVariantAnnotation] = {
    maybeLoadVcfAnnotations(filePath, predicate, projection, sd)
      .getOrElse({
        sd.foreach(sd => log.warn("Sequence dictionary for translation ignored if loading ADAM from Parquet."))
        adamLoad[DatabaseVariantAnnotation, U](filePath, predicate, projection)
      })
  }

  def loadFeatures[U <: ADAMPredicate[Feature]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None): RDD[Feature] = {
    maybeLoadBED(filePath).orElse(
      maybeLoadGTF(filePath)
    ).orElse(
        maybeLoadNarrowPeak(filePath)
      ).fold(adamLoad[Feature, U](filePath, predicate, projection))(applyPredicate(_, predicate))
  }

  def loadGenes[U <: ADAMPredicate[Feature]](filePath: String,
                                             predicate: Option[Class[U]] = None,
                                             projection: Option[Schema] = None): RDD[Gene] = {
    new GeneFeatureRDDFunctions(maybeLoadGTF(filePath)
      .fold(adamLoad[Feature, U](filePath, predicate, projection))(applyPredicate(_, predicate)))
      .asGenes()
  }

  def loadSequence[U <: ADAMPredicate[NucleotideContigFragment]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None,
    fragmentLength: Long = 10000): RDD[NucleotideContigFragment] = {
    maybeLoadFasta(filePath,
      predicate,
      projection,
      fragmentLength).getOrElse(
        adamLoad[NucleotideContigFragment, U](filePath, predicate, projection)
      )
  }

  def loadGenotypes[U <: ADAMPredicate[Genotype]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary] = None): RDD[Genotype] = {
    maybeLoadVcf(filePath, sd)
      .fold({
        sd.foreach(sd => log.warn("Sequence dictionary for translation ignored if loading ADAM from Parquet."))
        adamLoad[Genotype, U](filePath, predicate, projection)
      })(vcRdd => applyPredicate(vcRdd.flatMap(_.genotypes), predicate))
  }

  def loadVariants[U <: ADAMPredicate[Variant]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary] = None): RDD[Variant] = {
    maybeLoadVcf(filePath, sd)
      .fold({
        sd.foreach(sd => log.warn("Sequence dictionary for translation ignored if loading ADAM from Parquet."))
        adamLoad[Variant, U](filePath, predicate, projection)
      })(vcRdd => applyPredicate(vcRdd.map(_.variant.variant), predicate))
  }

  def loadAlignments[U <: ADAMPredicate[AlignmentRecord]](
    filePath: String,
    predicate: Option[Class[U]] = None,
    projection: Option[Schema] = None): RDD[AlignmentRecord] = LoadAlignmentRecords.time {

    val rdd = maybeLoadBam(filePath, predicate, projection)
      .orElse(
        maybeLoadFastq(filePath, predicate, projection)
      ).orElse(
          maybeLoadFasta(filePath, None, None, 10000).map(_.toReads)
            .map(applyPredicate(_, predicate)))
      .getOrElse(
        if (filePath.endsWith("contig.adam")) {
          applyPredicate(adamLoad[NucleotideContigFragment, UnboundRecordFilter](filePath).toReads, predicate)
        } else {
          adamLoad[AlignmentRecord, U](filePath, predicate, projection)
        })
    if (Metrics.isRecording) rdd.instrument() else rdd
  }

  /**
   * Takes a sequence of Path objects (e.g. the return value of findFiles).  Treats each path as
   * corresponding to a Read set -- loads each Read set, converts each set to use the
   * same SequenceDictionary, and returns the union of the RDDs.
   *
   * (GenomeBridge is using this to load BAMs that have been split into multiple files per sample,
   * for example, one-BAM-per-chromosome.)
   *
   * @param paths The locations of the parquet files to load
   * @return a single RDD[Read] that contains the union of the AlignmentRecords in the argument paths.
   */
  def loadAlignmentsFromPaths(paths: Seq[Path]): RDD[AlignmentRecord] = {
    sc.union(paths.map(p => loadAlignments(p.toString)))
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
