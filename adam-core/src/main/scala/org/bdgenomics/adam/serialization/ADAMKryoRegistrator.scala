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
package org.bdgenomics.adam.serialization

import com.esotericsoftware.kryo.io.{
  Input,
  KryoDataInput,
  KryoDataOutput,
  Output
}
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter, SpecificRecord }
import org.apache.hadoop.io.Writable
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.utils.misc.Logging
import scala.reflect.ClassTag

case class InputStreamWithDecoder(size: Int) {
  val buffer = new Array[Byte](size)
  val stream = new FastByteArrayInputStream(buffer)
  val decoder = DecoderFactory.get().directBinaryDecoder(stream, null.asInstanceOf[BinaryDecoder])
}

// NOTE: This class is not thread-safe; however, Spark guarantees that only a single thread will access it.
class AvroSerializer[T <: SpecificRecord: ClassTag] extends Serializer[T] {
  val reader = new SpecificDatumReader[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  val writer = new SpecificDatumWriter[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  var in = InputStreamWithDecoder(1024)
  val outstream = new FastByteArrayOutputStream()
  val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])

  setAcceptsNull(false)

  def write(kryo: Kryo, kryoOut: Output, record: T) = {
    outstream.reset()
    writer.write(record, encoder)
    kryoOut.writeInt(outstream.array.length, true)
    kryoOut.write(outstream.array)
  }

  def read(kryo: Kryo, kryoIn: Input, klazz: Class[T]): T = this.synchronized {
    val len = kryoIn.readInt(true)
    if (len > in.size) {
      in = InputStreamWithDecoder(len + 1024)
    }
    in.stream.reset()
    // Read Kryo bytes into input buffer
    kryoIn.readBytes(in.buffer, 0, len)
    // Read the Avro object from the buffer
    reader.read(null.asInstanceOf[T], in.decoder)
  }
}

/**
 * A Kryo serializer for Hadoop writables.
 *
 * Lifted from the Apache Spark user email list
 * (http://apache-spark-user-list.1001560.n3.nabble.com/Hadoop-Writable-and-Spark-serialization-td5721.html)
 * which indicates that it was originally copied from Shark itself, back when
 * Spark 0.9 was the state of the art.
 *
 * @tparam T The class to serialize, which implements the Writable interface.
 */
class WritableSerializer[T <: Writable] extends Serializer[T] {
  override def write(kryo: Kryo, output: Output, writable: T) {
    writable.write(new KryoDataOutput(output))
  }

  override def read(kryo: Kryo, input: Input, cls: java.lang.Class[T]): T = {
    val writable = cls.newInstance()
    writable.readFields(new KryoDataInput(input))
    writable
  }
}

class ADAMKryoRegistrator extends KryoRegistrator with Logging {
  override def registerClasses(kryo: Kryo) {

    // Register Avro classes using fully qualified class names
    // Sort alphabetically and add blank lines between packages

    // htsjdk.samtools
    kryo.register(classOf[htsjdk.samtools.CigarElement])
    kryo.register(classOf[htsjdk.samtools.CigarOperator])
    kryo.register(classOf[htsjdk.samtools.Cigar])
    kryo.register(classOf[htsjdk.samtools.SAMSequenceDictionary])
    kryo.register(classOf[htsjdk.samtools.SAMFileHeader])
    kryo.register(classOf[htsjdk.samtools.SAMSequenceRecord])

    // htsjdk.variant.vcf
    kryo.register(classOf[htsjdk.variant.vcf.VCFContigHeaderLine])
    kryo.register(classOf[htsjdk.variant.vcf.VCFFilterHeaderLine])
    kryo.register(classOf[htsjdk.variant.vcf.VCFFormatHeaderLine])
    kryo.register(classOf[htsjdk.variant.vcf.VCFInfoHeaderLine])
    kryo.register(classOf[htsjdk.variant.vcf.VCFHeader])
    kryo.register(classOf[htsjdk.variant.vcf.VCFHeaderLine])
    kryo.register(classOf[htsjdk.variant.vcf.VCFHeaderLineCount])
    kryo.register(classOf[htsjdk.variant.vcf.VCFHeaderLineType])
    kryo.register(Class.forName("htsjdk.variant.vcf.VCFCompoundHeaderLine$SupportedHeaderLineType"))

    // java.lang
    kryo.register(classOf[java.lang.Class[_]])

    // java.util
    kryo.register(classOf[java.util.ArrayList[_]])
    kryo.register(classOf[java.util.LinkedHashMap[_, _]])
    kryo.register(classOf[java.util.LinkedHashSet[_]])
    kryo.register(classOf[java.util.HashMap[_, _]])
    kryo.register(classOf[java.util.HashSet[_]])

    // org.apache.avro
    kryo.register(Class.forName("org.apache.avro.Schema$RecordSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$Field"))
    kryo.register(Class.forName("org.apache.avro.Schema$Field$Order"))
    kryo.register(Class.forName("org.apache.avro.Schema$UnionSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$Type"))
    kryo.register(Class.forName("org.apache.avro.Schema$LockableArrayList"))
    kryo.register(Class.forName("org.apache.avro.Schema$BooleanSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$NullSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$StringSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$IntSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$FloatSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$EnumSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$Name"))
    kryo.register(Class.forName("org.apache.avro.Schema$LongSchema"))
    kryo.register(Class.forName("org.apache.avro.generic.GenericData$Array"))

    // org.apache.hadoop.conf
    kryo.register(classOf[org.apache.hadoop.conf.Configuration],
      new WritableSerializer[org.apache.hadoop.conf.Configuration])
    kryo.register(classOf[org.apache.hadoop.yarn.conf.YarnConfiguration],
      new WritableSerializer[org.apache.hadoop.yarn.conf.YarnConfiguration])

    // org.apache.hadoop.io
    kryo.register(classOf[org.apache.hadoop.io.Text])
    kryo.register(classOf[org.apache.hadoop.io.LongWritable])

    // org.bdgenomics.adam.algorithms.consensus
    kryo.register(classOf[org.bdgenomics.adam.algorithms.consensus.Consensus])

    // org.bdgenomics.adam.converters
    kryo.register(classOf[org.bdgenomics.adam.converters.FastaConverter.FastaDescriptionLine])
    kryo.register(classOf[org.bdgenomics.adam.converters.FragmentCollector])

    // org.bdgenomics.adam.models
    kryo.register(classOf[org.bdgenomics.adam.models.Coverage])
    kryo.register(classOf[org.bdgenomics.adam.models.IndelTable])
    kryo.register(classOf[org.bdgenomics.adam.models.MdTag])
    kryo.register(classOf[org.bdgenomics.adam.models.MultiContigNonoverlappingRegions])
    kryo.register(classOf[org.bdgenomics.adam.models.NonoverlappingRegions])
    kryo.register(classOf[org.bdgenomics.adam.models.RecordGroup])
    kryo.register(classOf[org.bdgenomics.adam.models.RecordGroupDictionary])
    kryo.register(classOf[org.bdgenomics.adam.models.ReferencePosition],
      new org.bdgenomics.adam.models.ReferencePositionSerializer)
    kryo.register(classOf[org.bdgenomics.adam.models.ReferenceRegion])
    kryo.register(classOf[org.bdgenomics.adam.models.SAMFileHeaderWritable])
    kryo.register(classOf[org.bdgenomics.adam.models.SequenceDictionary])
    kryo.register(classOf[org.bdgenomics.adam.models.SequenceRecord])
    kryo.register(classOf[org.bdgenomics.adam.models.SnpTable],
      new org.bdgenomics.adam.models.SnpTableSerializer)
    kryo.register(classOf[org.bdgenomics.adam.models.VariantContext],
      new org.bdgenomics.adam.models.VariantContextSerializer)

    // org.bdgenomics.adam.rdd
    kryo.register(classOf[org.bdgenomics.adam.rdd.GenomeBins])

    // IntervalArray registrations for org.bdgenomics.adam.rdd
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.AlignmentRecordArray],
      new org.bdgenomics.adam.rdd.read.AlignmentRecordArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.feature.CoverageArray],
      new org.bdgenomics.adam.rdd.feature.CoverageArraySerializer(kryo))
    kryo.register(classOf[org.bdgenomics.adam.rdd.feature.FeatureArray],
      new org.bdgenomics.adam.rdd.feature.FeatureArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.fragment.FragmentArray],
      new org.bdgenomics.adam.rdd.fragment.FragmentArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.variant.GenotypeArray],
      new org.bdgenomics.adam.rdd.variant.GenotypeArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentArray],
      new org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.variant.VariantArray],
      new org.bdgenomics.adam.rdd.variant.VariantArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.variant.VariantContextArray],
      new org.bdgenomics.adam.rdd.variant.VariantContextArraySerializer)

    // org.bdgenomics.adam.rdd.read
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.FlagStatMetrics])
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.DuplicateMetrics])
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.SingleReadBucket],
      new org.bdgenomics.adam.rdd.read.SingleReadBucketSerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.ReferencePositionPair],
      new org.bdgenomics.adam.rdd.read.ReferencePositionPairSerializer)

    // org.bdgenomics.adam.rdd.read.realignment
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget],
      new org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTargetSerializer)
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget]],
      new org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTargetArraySerializer)
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.realignment.TargetSet],
      new org.bdgenomics.adam.rdd.read.realignment.TargetSetSerializer)

    // org.bdgenomics.adam.rdd.read.recalibration.
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.recalibration.CovariateKey])
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.recalibration.CycleCovariate])
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.recalibration.DinucCovariate])
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.recalibration.RecalibrationTable])
    kryo.register(classOf[org.bdgenomics.adam.rdd.read.recalibration.Observation])

    // org.bdgenomics.adam.rich
    kryo.register(classOf[org.bdgenomics.adam.rich.RichAlignmentRecord])
    kryo.register(classOf[org.bdgenomics.adam.rich.RichVariant])

    // org.bdgenomics.adam.util
    kryo.register(classOf[org.bdgenomics.adam.util.ReferenceContigMap],
      new org.bdgenomics.adam.util.ReferenceContigMapSerializer)
    kryo.register(classOf[org.bdgenomics.adam.util.TwoBitFile],
      new org.bdgenomics.adam.util.TwoBitFileSerializer)

    // org.bdgenomics.formats.avro
    kryo.register(classOf[org.bdgenomics.formats.avro.AlignmentRecord],
      new AvroSerializer[org.bdgenomics.formats.avro.AlignmentRecord])
    kryo.register(classOf[org.bdgenomics.formats.avro.Contig],
      new AvroSerializer[org.bdgenomics.formats.avro.Contig])
    kryo.register(classOf[org.bdgenomics.formats.avro.Dbxref],
      new AvroSerializer[org.bdgenomics.formats.avro.Dbxref])
    kryo.register(classOf[org.bdgenomics.formats.avro.Feature],
      new AvroSerializer[org.bdgenomics.formats.avro.Feature])
    kryo.register(classOf[org.bdgenomics.formats.avro.Fragment],
      new AvroSerializer[org.bdgenomics.formats.avro.Fragment])
    kryo.register(classOf[org.bdgenomics.formats.avro.Genotype],
      new AvroSerializer[org.bdgenomics.formats.avro.Genotype])
    kryo.register(classOf[org.bdgenomics.formats.avro.GenotypeAllele])
    kryo.register(classOf[org.bdgenomics.formats.avro.GenotypeType])
    kryo.register(classOf[org.bdgenomics.formats.avro.NucleotideContigFragment],
      new AvroSerializer[org.bdgenomics.formats.avro.NucleotideContigFragment])
    kryo.register(classOf[org.bdgenomics.formats.avro.OntologyTerm],
      new AvroSerializer[org.bdgenomics.formats.avro.OntologyTerm])
    kryo.register(classOf[org.bdgenomics.formats.avro.ProcessingStep],
      new AvroSerializer[org.bdgenomics.formats.avro.ProcessingStep])
    kryo.register(classOf[org.bdgenomics.formats.avro.Read],
      new AvroSerializer[org.bdgenomics.formats.avro.Read])
    kryo.register(classOf[org.bdgenomics.formats.avro.RecordGroup],
      new AvroSerializer[org.bdgenomics.formats.avro.RecordGroup])
    kryo.register(classOf[org.bdgenomics.formats.avro.Sample],
      new AvroSerializer[org.bdgenomics.formats.avro.Sample])
    kryo.register(classOf[org.bdgenomics.formats.avro.Sequence],
      new AvroSerializer[org.bdgenomics.formats.avro.Sequence])
    kryo.register(classOf[org.bdgenomics.formats.avro.Slice],
      new AvroSerializer[org.bdgenomics.formats.avro.Slice])
    kryo.register(classOf[org.bdgenomics.formats.avro.Strand])
    kryo.register(classOf[org.bdgenomics.formats.avro.TranscriptEffect],
      new AvroSerializer[org.bdgenomics.formats.avro.TranscriptEffect])
    kryo.register(classOf[org.bdgenomics.formats.avro.Variant],
      new AvroSerializer[org.bdgenomics.formats.avro.Variant])
    kryo.register(classOf[org.bdgenomics.formats.avro.VariantAnnotation],
      new AvroSerializer[org.bdgenomics.formats.avro.VariantAnnotation])
    kryo.register(classOf[org.bdgenomics.formats.avro.VariantAnnotationMessage])
    kryo.register(classOf[org.bdgenomics.formats.avro.VariantCallingAnnotations],
      new AvroSerializer[org.bdgenomics.formats.avro.VariantCallingAnnotations])

    // org.codehaus.jackson.node
    kryo.register(classOf[org.codehaus.jackson.node.NullNode])
    kryo.register(classOf[org.codehaus.jackson.node.BooleanNode])
    kryo.register(classOf[org.codehaus.jackson.node.TextNode])

    // org.apache.spark
    try {
      kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
      kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"))
    } catch {
      case cnfe: java.lang.ClassNotFoundException => {
        log.info("Did not find Spark internal class. This is expected for Spark 1.")
      }
    }
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])
    kryo.register(Class.forName("org.apache.spark.sql.types.BooleanType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.FloatType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"))
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(classOf[org.apache.spark.sql.types.MapType])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])

    // scala
    kryo.register(classOf[scala.Array[scala.Array[Byte]]])
    kryo.register(classOf[scala.Array[htsjdk.variant.vcf.VCFHeader]])
    kryo.register(classOf[scala.Array[java.lang.Integer]])
    kryo.register(classOf[scala.Array[java.lang.Long]])
    kryo.register(classOf[scala.Array[java.lang.Object]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.types.StructType]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.AlignmentRecord]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Contig]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Dbxref]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Feature]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Fragment]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Genotype]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.GenotypeAllele]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.OntologyTerm]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.NucleotideContigFragment]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Read]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.RecordGroup]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Sample]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Sequence]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Slice]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.TranscriptEffect]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.Variant]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.VariantAnnotation]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.VariantAnnotationMessage]])
    kryo.register(classOf[scala.Array[org.bdgenomics.formats.avro.VariantCallingAnnotations]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.algorithms.consensus.Consensus]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.Coverage]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.ReferencePosition]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.ReferenceRegion]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.SequenceRecord]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.VariantContext]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.rdd.read.recalibration.CovariateKey]])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.rich.RichAlignmentRecord]])
    kryo.register(classOf[scala.Array[scala.collection.Seq[_]]])
    kryo.register(classOf[scala.Array[Int]])
    kryo.register(classOf[scala.Array[Long]])
    kryo.register(classOf[scala.Array[String]])
    kryo.register(classOf[scala.Array[Option[_]]])
    kryo.register(Class.forName("scala.Tuple2$mcCC$sp"))

    // scala.collection
    kryo.register(Class.forName("scala.collection.Iterator$$anon$11"))
    kryo.register(Class.forName("scala.collection.Iterator$$anonfun$toStream$1"))

    // scala.collection.convert
    kryo.register(Class.forName("scala.collection.convert.Wrappers$"))

    // scala.collection.immutable
    kryo.register(classOf[scala.collection.immutable.::[_]])
    kryo.register(classOf[scala.collection.immutable.Range])
    kryo.register(Class.forName("scala.collection.immutable.Stream$Cons"))
    kryo.register(Class.forName("scala.collection.immutable.Stream$Empty$"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))

    // scala.collection.mutable
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.collection.mutable.ListBuffer[_]])
    kryo.register(Class.forName("scala.collection.mutable.ListBuffer$$anon$1"))
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    // scala.math
    kryo.register(scala.math.Numeric.LongIsIntegral.getClass)

    // This seems to be necessary when serializing a RangePartitioner, which writes out a ClassTag:
    //
    //  https://github.com/apache/spark/blob/v1.5.2/core/src/main/scala/org/apache/spark/Partitioner.scala#L220
    //
    // See also:
    //
    //   https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))

    // needed for manifests
    kryo.register(Class.forName("scala.reflect.ManifestFactory$ClassTypeManifest"))

    // Added to Spark in 1.6.0; needed here for Spark < 1.6.0.
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[(Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])

    kryo.register(Map.empty.getClass)
    kryo.register(Nil.getClass)
    kryo.register(None.getClass)
  }
}
