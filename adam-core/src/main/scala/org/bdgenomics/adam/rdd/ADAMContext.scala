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
import parquet.filter2.predicate.FilterPredicate
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
  private[rdd] def adamLoad[T](filePath: String, predicate: Option[FilterPredicate] = None, projection: Option[Schema] = None)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): RDD[T] = {
    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    log.info("Reading the ADAM file at %s to create RDD".format(filePath))
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])

    if (predicate.isDefined) {
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, predicate.get)
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
      val projected: RDD[T] = adamLoad[T](filePath, None, projection = Some(projection))

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

  def loadBam(
    filePath: String): RDD[AlignmentRecord] = {

    // We need to separately read the header, so that we can inject the sequence dictionary
    // data into each individual Read (see the argument to samRecordConverter.convert,
    // below).
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration)
    val seqDict = adamBamDictionaryLoad(samHeader)
    val readGroups = adamBamLoadReadGroups(samHeader)

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    if (Metrics.isRecording) records.instrument() else records
    val samRecordConverter = new SAMRecordConverter

    records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups))
  }

  def loadParquetAlignments(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): RDD[AlignmentRecord] = {
    adamLoad[AlignmentRecord](filePath, predicate, projection)
  }

  def loadInterleavedFastq(
    filePath: String): RDD[AlignmentRecord] = {

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
    records.flatMap(fastqRecordConverter.convertPair)
  }

  def loadUnpairedFastq(
    filePath: String): RDD[AlignmentRecord] = {

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
    records.map(fastqRecordConverter.convertRead)
  }

  def loadVcf(filePath: String, sd: Option[SequenceDictionary]): RDD[VariantContext] = {
    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter(sd)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))
    if (Metrics.isRecording) records.instrument() else records

    records.flatMap(p => vcc.convert(p._2.get))
  }

  def loadParquetGenotypes(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): RDD[Genotype] = {
    adamLoad[Genotype](filePath, predicate, projection)
  }

  def loadParquetVariants(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): RDD[Variant] = {
    adamLoad[Variant](filePath, predicate, projection)
  }

  def loadFasta(
    filePath: String,
    fragmentLength: Long): RDD[NucleotideContigFragment] = {
    val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(filePath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text])
    if (Metrics.isRecording) fastaData.instrument() else fastaData

    val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

    FastaConverter(remapData, fragmentLength)
  }

  def loadParquetFragments(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): RDD[NucleotideContigFragment] = {
    adamLoad[NucleotideContigFragment](filePath, predicate, projection)
  }

  def loadGTF(filePath: String): RDD[Feature] = {
    val records = sc.textFile(filePath).flatMap(new GTFParser().parse)
    if (Metrics.isRecording) records.instrument() else records
  }

  def loadBED(filePath: String): RDD[Feature] = {
    val records = sc.textFile(filePath).flatMap(new BEDParser().parse)
    if (Metrics.isRecording) records.instrument() else records
  }

  def loadNarrowPeak(filePath: String): RDD[Feature] = {
    val records = sc.textFile(filePath).flatMap(new NarrowPeakParser().parse)
    if (Metrics.isRecording) records.instrument() else records
  }

  def loadParquetFeatures(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): RDD[Feature] = {
    adamLoad[Feature](filePath, predicate, projection)
  }

  def loadVcfAnnotations(
    filePath: String,
    sd: Option[SequenceDictionary] = None): RDD[DatabaseVariantAnnotation] = {

    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter(sd)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))
    if (Metrics.isRecording) records.instrument() else records

    records.map(p => vcc.convertToAnnotation(p._2.get))
  }

  def loadParquetVariantAnnotations(
    filePath: String,
    predicate: Option[FilterPredicate] = None,
    projection: Option[Schema] = None): RDD[DatabaseVariantAnnotation] = {
    adamLoad[DatabaseVariantAnnotation](filePath, predicate, projection)
  }

  def loadVariantAnnotations(
    filePath: String,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary] = None): RDD[DatabaseVariantAnnotation] = {
    if (filePath.endsWith(".vcf")) {
      log.info("Loading " + filePath + " as VCF, and converting to variant annotations. Projection is ignored.")
      loadVcfAnnotations(filePath, sd)
    } else {
      log.info("Loading " + filePath + " as Parquet containing DatabaseVariantAnnotations.")
      sd.foreach(sd => log.warn("Sequence dictionary for translation ignored if loading ADAM from Parquet."))
      loadParquetVariantAnnotations(filePath, None, projection)
    }
  }

  def loadFeatures(
    filePath: String,
    projection: Option[Schema] = None): RDD[Feature] = {

    if (filePath.endsWith(".bed")) {
      log.info("Loading " + filePath + " as BED and converting to features. Projection is ignored.")
      loadBED(filePath)
    } else if (filePath.endsWith(".gtf") ||
      filePath.endsWith(".gff")) {
      log.info("Loading " + filePath + " as GTF/GFF and converting to features. Projection is ignored.")
      loadGTF(filePath)
    } else if (filePath.endsWith(".narrowPeak") ||
      filePath.endsWith(".narrowpeak")) {
      log.info("Loading " + filePath + " as NarrowPeak and converting to features. Projection is ignored.")
      loadNarrowPeak(filePath)
    } else {
      log.info("Loading " + filePath + " as Parquet containing Features.")
      loadParquetFeatures(filePath, None, projection)
    }
  }

  def loadGenes(filePath: String,
                projection: Option[Schema] = None): RDD[Gene] = {
    import ADAMContext._
    loadFeatures(filePath, projection).asGenes()
  }

  def loadSequence(
    filePath: String,
    projection: Option[Schema] = None,
    fragmentLength: Long = 10000): RDD[NucleotideContigFragment] = {
    if (filePath.endsWith(".fa") ||
      filePath.endsWith(".fasta")) {
      log.info("Loading " + filePath + " as FASTA and converting to NucleotideContigFragment. Projection is ignored.")
      loadFasta(filePath,
        fragmentLength)
    } else {
      log.info("Loading " + filePath + " as Parquet containing NucleotideContigFragments.")
      loadParquetFragments(filePath, None, projection)
    }
  }

  def loadGenotypes(
    filePath: String,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary] = None): RDD[Genotype] = {
    if (filePath.endsWith(".vcf")) {
      log.info("Loading " + filePath + " as VCF, and converting to Genotypes. Projection is ignored.")
      loadVcf(filePath, sd).flatMap(_.genotypes)
    } else {
      log.info("Loading " + filePath + " as Parquet containing Genotypes. Sequence dictionary for translation is ignored.")
      loadParquetGenotypes(filePath, None, projection)
    }
  }

  def loadVariants(
    filePath: String,
    projection: Option[Schema] = None,
    sd: Option[SequenceDictionary] = None): RDD[Variant] = {
    if (filePath.endsWith(".vcf")) {
      log.info("Loading " + filePath + " as VCF, and converting to Variants. Projection is ignored.")
      loadVcf(filePath, sd).map(_.variant.variant)
    } else {
      log.info("Loading " + filePath + " as Parquet containing Variants. Sequence dictionary for translation is ignored.")
      loadParquetVariants(filePath, None, projection)
    }
  }

  def loadAlignments(
    filePath: String,
    projection: Option[Schema] = None): RDD[AlignmentRecord] = LoadAlignmentRecords.time {

    if (filePath.endsWith(".sam") ||
      filePath.endsWith(".bam")) {
      log.info("Loading " + filePath + " as SAM/BAM and converting to AlignmentRecords. Projection is ignored.")
      loadBam(filePath)
    } else if (filePath.endsWith(".ifq")) {
      log.info("Loading " + filePath + " as interleaved FASTQ and converting to AlignmentRecords. Projection is ignored.")
      loadInterleavedFastq(filePath)
    } else if (filePath.endsWith(".fq") ||
      filePath.endsWith(".fastq")) {
      log.info("Loading " + filePath + " as unpaired FASTQ and converting to AlignmentRecords. Projection is ignored.")
      loadUnpairedFastq(filePath)
    } else if (filePath.endsWith(".fa") ||
      filePath.endsWith(".fasta")) {
      log.info("Loading " + filePath + " as FASTA and converting to AlignmentRecords. Projection is ignored.")
      import ADAMContext._
      loadFasta(filePath, fragmentLength = 10000).toReads
    } else if (filePath.endsWith("contig.adam")) {
      log.info("Loading " + filePath + " as Parquet of NucleotideContigFragment and converting to AlignmentRecords. Projection is ignored.")
      adamLoad[NucleotideContigFragment](filePath).toReads
    } else {
      log.info("Loading " + filePath + " as Parquet of AlignmentRecords.")
      loadParquetAlignments(filePath, None, projection)
    }
  }

  /**
   * Takes a sequence of Path objects (e.g. the return value of findFiles).  Treats each path as
   * corresponding to a Read set -- loads each Read set, converts each set to use the
   * same SequenceDictionary, and returns the union of the RDDs.
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
