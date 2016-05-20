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

import java.util
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import htsjdk.samtools.{
  Cigar,
  CigarElement,
  CigarOperator,
  SAMFileHeader,
  SAMSequenceDictionary,
  SAMSequenceRecord
}
import htsjdk.variant.vcf.{
  VCFContigHeaderLine,
  VCFFilterHeaderLine,
  VCFFormatHeaderLine,
  VCFInfoHeaderLine,
  VCFHeader,
  VCFHeaderLine,
  VCFHeaderLineCount,
  VCFHeaderLineType
}
import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter, SpecificRecord }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.converters.FastaConverter.FastaDescriptionLine
import org.bdgenomics.adam.converters.FragmentCollector
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.read.realignment._
import org.bdgenomics.adam.rdd.read.recalibration.{ CovariateKey, CovariateSpace, CycleCovariate, DinucCovariate, Observation, ObservationAccumulator }
import org.bdgenomics.adam.rdd.read.{ DuplicateMetrics, FlagStatMetrics }
import org.bdgenomics.adam.rdd.{ GenomeBins, OrientedPoint }
import org.bdgenomics.adam.rich.{ DecadentRead, ReferenceSequenceContext, RichAlignmentRecord, RichVariant }
import org.bdgenomics.adam.util.{ MdTag, QualityScore, ReferenceContigMap, TwoBitFile, TwoBitFileSerializer }
import org.bdgenomics.formats.avro._
import org.codehaus.jackson.node.{ BooleanNode, NullNode, TextNode }
import org.seqdoop.hadoop_bam.{ VariantContextWithHeader, VariantContextWritable }
import scala.collection.immutable
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

class ADAMKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // Register Avro classes
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[AlignmentRecord])
    kryo.register(classOf[Array[AlignmentRecord]])

    kryo.register(classOf[Genotype], new AvroSerializer[Genotype])
    kryo.register(classOf[Array[Genotype]])

    kryo.register(classOf[Variant], new AvroSerializer[Variant])
    kryo.register(classOf[Array[Variant]])

    kryo.register(classOf[DatabaseVariantAnnotation], new AvroSerializer[DatabaseVariantAnnotation])
    kryo.register(classOf[Array[DatabaseVariantAnnotation]])

    kryo.register(classOf[NucleotideContigFragment], new AvroSerializer[NucleotideContigFragment])
    kryo.register(classOf[Array[NucleotideContigFragment]])

    kryo.register(classOf[Contig], new AvroSerializer[Contig])
    kryo.register(classOf[RecordGroupMetadata], new AvroSerializer[RecordGroupMetadata])
    kryo.register(classOf[StructuralVariant], new AvroSerializer[StructuralVariant])
    kryo.register(classOf[VariantCallingAnnotations], new AvroSerializer[VariantCallingAnnotations])
    kryo.register(classOf[TranscriptEffect], new AvroSerializer[TranscriptEffect])
    kryo.register(classOf[VariantAnnotation], new AvroSerializer[VariantAnnotation])

    kryo.register(classOf[DatabaseVariantAnnotation], new AvroSerializer[DatabaseVariantAnnotation])
    kryo.register(classOf[Dbxref], new AvroSerializer[Dbxref])

    kryo.register(classOf[Strand])

    kryo.register(classOf[Feature], new AvroSerializer[Feature])
    kryo.register(classOf[Array[Feature]])

    kryo.register(classOf[ReferencePosition], new ReferencePositionSerializer)
    kryo.register(classOf[Array[ReferencePosition]])

    kryo.register(classOf[ReferenceRegion])
    kryo.register(classOf[Array[ReferenceRegion]])

    kryo.register(classOf[ReferencePositionPair], new ReferencePositionPairSerializer)
    kryo.register(classOf[SingleReadBucket], new SingleReadBucketSerializer)

    kryo.register(classOf[IndelRealignmentTarget])
    kryo.register(classOf[TargetSet], new TargetSetSerializer)
    kryo.register(classOf[ZippedTargetSet], new ZippedTargetSetSerializer)

    // Serialized in RealignmentTargetFinder.
    kryo.register(classOf[immutable.::[_]])

    kryo.register(classOf[TwoBitFile], new TwoBitFileSerializer)
    kryo.register(classOf[ReferenceContigMap])

    // Broadcasted in ShuffleRegionJoin.
    kryo.register(classOf[GenomeBins])

    // Collected in ADAMContext.loadIntervalList.
    kryo.register(classOf[SequenceRecord])
    kryo.register(classOf[Array[SequenceRecord]])

    // Reduced in ADAMSequenceDictionaryRDDAggregator.getSequenceDictionary.
    kryo.register(classOf[SequenceDictionary])

    // Collected in ADAMContextSuite / GenotypeRDDFunctions.toVariantContext.
    kryo.register(classOf[RichVariant])
    kryo.register(classOf[Array[VariantContext]])
    kryo.register(classOf[VariantContext])

    // collected in ADAMContext.loadVcfMetadata
    // from htsjdk
    kryo.register(classOf[VCFContigHeaderLine])
    kryo.register(classOf[VCFFilterHeaderLine])
    kryo.register(classOf[VCFFormatHeaderLine])
    kryo.register(classOf[VCFInfoHeaderLine])
    kryo.register(classOf[Array[VCFHeader]])
    kryo.register(classOf[VCFHeader])
    kryo.register(classOf[VCFHeaderLine])
    kryo.register(classOf[VCFHeaderLineCount])
    kryo.register(classOf[VCFHeaderLineType])
    kryo.register(Class.forName("htsjdk.variant.vcf.VCFCompoundHeaderLine$SupportedHeaderLineType"))

    // Serialized in GeneSuite / FeatureRDDFunctions.toGene.
    kryo.register(classOf[Exon])
    kryo.register(classOf[Array[Exon]])
    kryo.register(classOf[UTR])
    kryo.register(classOf[CDS])
    kryo.register(classOf[Array[Transcript]])
    kryo.register(classOf[Transcript])

    // Broadcast in AlignmentRecordRDDFunctions.convertToSam
    kryo.register(classOf[SAMFileHeaderWritable])

    kryo.register(classOf[RichAlignmentRecord])
    kryo.register(classOf[Array[RichAlignmentRecord]])
    kryo.register(classOf[ReferenceSequenceContext])

    kryo.register(classOf[DecadentRead])

    kryo.register(classOf[FastaDescriptionLine])

    // Serialized when manipulating FASTA lines.
    kryo.register(classOf[Array[String]])

    // Used in PairingRDD.sliding.
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[Array[Seq[_]]])
    kryo.register(classOf[Range])
    kryo.register(classOf[Array[Object]])

    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    kryo.register(classOf[SnpTable])
    kryo.register(classOf[MultiContigNonoverlappingRegions])
    kryo.register(classOf[MdTag])
    kryo.register(classOf[FragmentCollector])
    kryo.register(classOf[Text])
    kryo.register(classOf[LongWritable])

    kryo.register(classOf[SAMSequenceDictionary])
    kryo.register(classOf[SAMFileHeader])
    kryo.register(classOf[SAMSequenceRecord])
    kryo.register(Class.forName("scala.collection.convert.Wrappers$"))

    kryo.register(classOf[CigarElement])
    kryo.register(classOf[CigarOperator])
    kryo.register(classOf[Cigar])

    kryo.register(classOf[Consensus])
    kryo.register(classOf[NonoverlappingRegions])

    kryo.register(classOf[RecordGroupDictionary])
    kryo.register(classOf[RecordGroup])

    kryo.register(classOf[FlagStatMetrics])
    kryo.register(classOf[DuplicateMetrics])

    kryo.register(Class.forName("scala.Tuple2$mcCC$sp"))

    kryo.register(classOf[CovariateSpace])
    kryo.register(classOf[CycleCovariate])
    kryo.register(classOf[DinucCovariate])
    kryo.register(classOf[CovariateKey])
    kryo.register(classOf[ObservationAccumulator])
    kryo.register(classOf[Observation])

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

    kryo.register(classOf[NullNode])
    kryo.register(classOf[BooleanNode])
    kryo.register(classOf[TextNode])

    kryo.register(classOf[QualityScore])

    kryo.register(classOf[util.ArrayList[_]])
    kryo.register(classOf[util.LinkedHashMap[_, _]])
    kryo.register(classOf[util.LinkedHashSet[_]])
    kryo.register(classOf[util.HashMap[_, _]])
    kryo.register(classOf[util.HashSet[_]])

    kryo.register(classOf[Array[OrientedPoint]])
    kryo.register(classOf[OrientedPoint])

    kryo.register(scala.math.Numeric.LongIsIntegral.getClass)
    kryo.register(Map.empty.getClass)

    // This seems to be necessary when serializing a RangePartitioner, which writes out a ClassTag:
    //
    //  https://github.com/apache/spark/blob/v1.5.2/core/src/main/scala/org/apache/spark/Partitioner.scala#L220
    //
    // See also:
    //
    //   https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    // Added to Spark in 1.6.0; needed here for Spark < 1.6.0.
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[Tuple2[Any, Any]]])
    kryo.register(classOf[Array[Tuple3[Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple4[Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple5[Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple6[Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple7[Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])

    kryo.register(None.getClass)
    kryo.register(Nil.getClass)
  }
}
