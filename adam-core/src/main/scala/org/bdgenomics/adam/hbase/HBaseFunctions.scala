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
package org.bdgenomics.adam.hbase

import java.io.ByteArrayOutputStream
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.fs.{ FSDataInputStream, FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.variation.{ VariantContextRDD }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.models.{ ReferencePosition, ReferenceRegion, SequenceDictionary, VariantContext }
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import sys.process._

object HBaseFunctions {

  /**
   * HBase Spark Data Access Object
   *
   * In standard usage user will create a single instance of this object, and pass it as the first parameter
   * when using the HBaseFunctions public API functions
   *
   * @param sc SparkContext
   * @param inConf optional HBase Configuration parameter, only used if manually configuring HBase connection
   * @param inConnection optional HBase Connection parameter, only used if manually configuring HBase connection
   * @param inAdmin optional HBase Admin parameter, only used if manually configuring HBase connection
   * @param inHBaseContext optional HBaseContext parameter, only used if manually configuring HBase connection
   */

  class HBaseSparkDAO(sc: SparkContext,
                      inConf: Option[Configuration] = None,
                      inConnection: Option[Connection] = None,
                      inAdmin: Option[Admin] = None,
                      inHBaseContext: Option[HBaseContext] = None) {

    val conf = if (inConf.isDefined) inConf.get else HBaseConfiguration.create()
    val connection = if (inConnection.isDefined) inConnection.get else ConnectionFactory.createConnection(conf)
    val admin = if (inAdmin.isDefined) inAdmin.get else connection.getAdmin
    val hbaseContext = if (inHBaseContext.isDefined) inHBaseContext.get else new HBaseContext(sc, conf)

    def createTable(tableDescriptorMeta: HTableDescriptor) { admin.createTable(tableDescriptorMeta) }
    def getTable(tableName: TableName): Table = { connection.getTable(tableName) }

    def getHBaseRDD(tableName: org.apache.hadoop.hbase.TableName,
                    scans: org.apache.hadoop.hbase.client.Scan): RDD[scala.Tuple2[org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result]] = {
      hbaseContext.hbaseRDD(tableName, scans)

    }

    def hbaseBulkLoad(genodata: RDD[(Array[Byte], List[(String, Array[Byte])])],
                      hbaseTableName: String,
                      flatMap: scala.Function1[(Array[Byte], List[(String, Array[Byte])]), scala.Iterator[scala.Tuple2[org.apache.hadoop.hbase.spark.KeyFamilyQualifier, scala.Array[scala.Byte]]]],
                      stagingFolder: String) = {

      val familyHBaseWriterOptions = new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions]
      val f1Options = new FamilyHFileWriteOptions("GZ", "ROW", 128, "FAST_DIFF")
      familyHBaseWriterOptions.put(Bytes.toBytes("g"), f1Options)

      // call implicit RDD function hbaseBulkLoad to write bulk files to staging locations
      // and then load to HBase
      genodata.hbaseBulkLoad(hbaseContext,
        TableName.valueOf(hbaseTableName),
        flatMap,
        stagingFolder,
        familyHBaseWriterOptions,
        compactionExclude = false,
        HConstants.DEFAULT_MAX_FILE_SIZE)

      // This permission change appears necessary, plan to revisit to find better way
      ("hadoop fs -chmod -R 660 " + stagingFolder) !

      val load = new LoadIncrementalHFiles(conf)
      load.doBulkLoad(new Path(stagingFolder), admin, connection.getTable(TableName.valueOf(hbaseTableName)),
        connection.getRegionLocator(TableName.valueOf(hbaseTableName)))

    }

    def hbaseBulkDelete() {}

  }

  /**
   * KeyStrategy abstract class
   *
   * @tparam T Type of rowKeyInfo class which is container for data needed to construct a row key
   * @tparam U Type of rangePrefixInfo class which is a container for the data needed to define a key range
   */
  private[hbase] abstract class KeyStrategy[T, U] {
    def getKey(rowKeyInfo: T): Array[Byte]
    def getKeyRangePrefix(rangePrefixInfo: U): (Array[Byte], Array[Byte])
  }

  private[hbase] case class KeyStrategy1rowKeyInfo(contigName: String,
                                                   start: Int,
                                                   end: Int,
                                                   refAllele: String,
                                                   altAllele: String)

  /**
   * KeyStrategy1RangePrefixInfo contains a ReferenceRegion used to define a key range prefix in
   * Keystrategy1
   *
   * @param queryRegion
   */
  private[hbase] case class KeyStrategy1RangePrefixInfo(queryRegion: ReferenceRegion)

  /**
   * KeyStrategy1  implements a HBase row key design consisting of underscore seperated fields:
   *   contigName
   *   start
   *   refAlleel
   *   AltAllele
   *   length
   *
   */

  private[hbase] object KeyStrategy1 extends KeyStrategy[KeyStrategy1rowKeyInfo, KeyStrategy1RangePrefixInfo] {
    def getKey(rowKeyInfo: KeyStrategy1rowKeyInfo): Array[Byte] = {
      Bytes.toBytes(rowKeyInfo.contigName + "_" +
        String.format("%10s", rowKeyInfo.start.toString).replace(' ', '0') + "_" +
        rowKeyInfo.refAllele + "_" +
        rowKeyInfo.altAllele + "_" +
        (rowKeyInfo.end - rowKeyInfo.start).toString)
    }

    def getKeyRangePrefix(rangePrefixInfo: KeyStrategy1RangePrefixInfo): (Array[Byte], Array[Byte]) = {
      val start = Bytes.toBytes(rangePrefixInfo.queryRegion.referenceName + "_" +
        String.format("%10s", rangePrefixInfo.queryRegion.start.toString).replace(' ', '0'))
      val stop = Bytes.toBytes(rangePrefixInfo.queryRegion.referenceName + "_" +
        String.format("%10s", rangePrefixInfo.queryRegion.end.toString).replace(' ', '0'))
      (start, stop)
    }
  }

  /**
   * saves Sequence Dictionary data to HBase
   *
   * @param dao
   * @param hbaseTableName
   * @param sequences
   * @param sequenceDictionaryId
   */

  private[hbase] def saveSequenceDictionaryToHBase(dao: HBaseSparkDAO,
                                                   hbaseTableName: String,
                                                   sequences: SequenceDictionary,
                                                   sequenceDictionaryId: String): Unit = {

    val table = dao.getTable(TableName.valueOf(hbaseTableName))
    val contigs = sequences.toAvro

    val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)

    val contigDatumWriter: DatumWriter[Contig] =
      new SpecificDatumWriter[Contig](scala.reflect.classTag[Contig].runtimeClass.asInstanceOf[Class[Contig]])
    contigs.foreach((x) => {

      contigDatumWriter.write(x, encoder)
    })

    encoder.flush()
    baos.flush()

    val put: Put = new Put(Bytes.toBytes(sequenceDictionaryId))
    put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("contig"), baos.toByteArray)
    table.put(put)
  }

  /**
   * loads Sequence Dictionary data from HBase
   *
   * @param dao
   * @param HbaseTableName
   * @param sequenceDictionaryId
   * @return
   */

  private[hbase] def loadSequenceDictionaryFromHBase(dao: HBaseSparkDAO,
                                                     HbaseTableName: String,
                                                     sequenceDictionaryId: String): SequenceDictionary = {

    val table = dao.getTable(TableName.valueOf(HbaseTableName))

    val myGet = new Get(Bytes.toBytes(sequenceDictionaryId))
    val result = table.get(myGet)
    val dataBytes = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("contig"))

    val contigDatumReader: DatumReader[Contig] =
      new SpecificDatumReader[Contig](scala.reflect.classTag[Contig]
        .runtimeClass
        .asInstanceOf[Class[Contig]])

    val decoder = DecoderFactory.get().binaryDecoder(dataBytes, null)
    var resultList = new ListBuffer[Contig]
    while (!decoder.isEnd) {
      resultList += contigDatumReader.read(null, decoder)
    }

    SequenceDictionary.fromAvro(resultList)

  }

  /**
   * save Sample Metadata to HBase
   *
   * @param dao
   * @param hbaseTableName
   * @param samples
   */

  private[hbase] def saveSampleMetadataToHBase(dao: HBaseSparkDAO,
                                               hbaseTableName: String,
                                               samples: Seq[Sample]): Unit = {

    val table: Table = dao.getTable(TableName.valueOf(hbaseTableName))

    val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)
    val sampleDatumWriter: DatumWriter[Sample] =
      new SpecificDatumWriter[Sample](scala.reflect.classTag[Sample].runtimeClass.asInstanceOf[Class[Sample]])
    samples.foreach((x) => {
      sampleDatumWriter.write(x, encoder)
      encoder.flush()
      baos.flush()

      val curr_sampleid = x.getSampleId

      val put = new Put(Bytes.toBytes(curr_sampleid))
      put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("sampledata"), baos.toByteArray)
      table.put(put)

    })
  }

  /**
   * load sample Metadata from HBase
   *
   * @param dao
   * @param hbaseTableName
   * @param sampleIds
   * @return
   */
  private[hbase] def loadSampleMetadataFromHBase(dao: HBaseSparkDAO,
                                                 hbaseTableName: String,
                                                 sampleIds: Seq[String]): Seq[Sample] = {

    val table = dao.getTable(TableName.valueOf(hbaseTableName))

    var resultList = new ListBuffer[Sample]

    sampleIds.foreach((sampleId) => {
      val get = new Get(Bytes.toBytes(sampleId))
      val result = table.get(get)
      val dataBytes = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("sampledata"))

      val sampleDatumReader: DatumReader[Sample] =
        new SpecificDatumReader[Sample](scala.reflect.classTag[Sample]
          .runtimeClass
          .asInstanceOf[Class[Sample]])

      val decoder = DecoderFactory.get().binaryDecoder(dataBytes, null)

      while (!decoder.isEnd) {
        resultList += sampleDatumReader.read(null, decoder)
      }

    })
    resultList
  }

  /**
   *
   * @param dao HBase Data Access Object
   * @param hbaseTableName HBase Table Name
   * @param sampleIds Sample Id list
   * @param sampleListFile File containing sample IDs, one per line
   * @param queryRegion Genomic ReferenceRegion used to limit query
   * @param partitions number of partitions in which to repartition RDD
   * @return
   */

  private[hbase] def loadVariantContextsFromHBase(dao: HBaseSparkDAO,
                                                  hbaseTableName: String,
                                                  sampleIds: Option[List[String]] = None,
                                                  sampleListFile: Option[String] = None,
                                                  queryRegion: Option[ReferenceRegion] = None,
                                                  partitions: Option[Int] = None,
                                                  cachingValue: Int = 100): RDD[VariantContext] = {

    val scan = new Scan()

    scan.setCaching(cachingValue)
    scan.setMaxVersions(1)

    queryRegion.foreach { (currQueryRegion) =>
      {

        val (start, stop) = KeyStrategy1.getKeyRangePrefix(KeyStrategy1RangePrefixInfo(currQueryRegion))
        scan.setStartRow(start)
        scan.setStopRow(stop)
      }
    }

    val sampleIdsFinal: List[String] =
      if (sampleListFile.isDefined) {
        val sampleIdsLst: List[String] = Source.fromFile(sampleListFile.get).getLines.toList
        sampleIdsLst.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIdsLst
      } else {
        sampleIds.get.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIds.get
      }

    val resultHBaseRDD: RDD[(ImmutableBytesWritable, Result)] = dao.getHBaseRDD(TableName.valueOf(hbaseTableName), scan)
    val resultHBaseRDDrepar = if (partitions.isDefined) resultHBaseRDD.repartition(partitions.get)
    else resultHBaseRDD

    val result: RDD[VariantContext] = resultHBaseRDDrepar.mapPartitions((iterator) => {

      val genotypeDatumReader: DatumReader[Genotype] =
        new SpecificDatumReader[Genotype](scala.reflect.classTag[Genotype]
          .runtimeClass
          .asInstanceOf[Class[Genotype]])

      iterator.map((curr) => {
        var result = new ListBuffer[Genotype]
        sampleIdsFinal.foreach((sample) => {
          val sampleVal = curr._2.getColumnCells(Bytes.toBytes("g"), Bytes.toBytes(sample))
          val decoder = DecoderFactory.get().binaryDecoder(CellUtil.cloneValue(sampleVal.get(0)), null)
          while (!decoder.isEnd) {
            result += genotypeDatumReader.read(null, decoder)
          }
        })

        result
        val x = result.toList

        val firstGenotype: Genotype = x.head
        val currVar: RichVariant = RichVariant.genotypeToRichVariant(firstGenotype)
        val currRefPos = ReferencePosition(currVar.variant.getContigName, currVar.variant.getStart)

        val currVariantContext: VariantContext = new VariantContext(currRefPos, currVar, x)
        currVariantContext
      })

    })

    result
  }

  /**
   *
   * @param dao HBase Data Access Object
   * @param hbaseTableName HBase Table Name
   * @param splitsFileName File path and name for file with pre-defined splits of HBase key space
   */

  def createHBaseGenotypeTable(dao: HBaseSparkDAO, hbaseTableName: String, splitsFileName: Option[String] = None) {

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.addFamily(new HColumnDescriptor("g".getBytes()).setCompressionType(Algorithm.GZ)
      .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      .setMaxVersions(1))

    if (splitsFileName.isDefined) {

      // splits defines a set of redefined HBase region splits based on an array of Array[Byte] indicating the
      // split points
      val splits: Array[Array[Byte]] = Source.fromFile(splitsFileName.get).getLines.toArray
        .map(x => x.split(" "))
        .map(g => Bytes.toBytes(g(0) + "_"
          + String.format("%10s", g(1)).replace(' ', '0')))

      dao.admin.createTable(tableDescriptor, splits)
    } else dao.admin.createTable(tableDescriptor)

    val hbaseTableNameMeta = hbaseTableName + "_meta"

    val tableDescriptorMeta: HTableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableNameMeta))
    tableDescriptorMeta.addFamily(new HColumnDescriptor("meta".getBytes())
      .setCompressionType(Algorithm.GZ)
      .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      .setMaxVersions(1))
    dao.admin.createTable(tableDescriptorMeta)
  }

  /**
   *
   * @param dao HBase Data Access Object
   * @param vcRdd VariantContextRDD from which to save data to HBase
   * @param hbaseTableName HBase table name
   * @param sequenceDictionaryId user defined sequence Dionctionary Id to use when saving
   * @param saveSequenceDictionary toggle saving of Sequence Dictioanry, set to false when relevant Sequence
   *                               Dictionary has already been saved to HBase in a previous load operation
   * @param partitions - number N of partitions in which to repartition data, if not defined data is not repartitioned
   * @param stagingFolder - user defined location (on HDFS) to which to save temporary staging files
   */

  def saveVariantContextRDDToHBaseBulk(dao: HBaseSparkDAO,
                                       vcRdd: VariantContextRDD,
                                       hbaseTableName: String,
                                       sequenceDictionaryId: String,
                                       stagingFolder: String,
                                       saveSequenceDictionary: Boolean = true,
                                       partitions: Option[Int] = None) = {

    saveSampleMetadataToHBase(dao, hbaseTableName + "_meta", vcRdd.samples)

    if (saveSequenceDictionary) saveSequenceDictionaryToHBase(dao,
      hbaseTableName + "_meta",
      vcRdd.sequences,
      sequenceDictionaryId)

    val rdd = if (partitions.isDefined) vcRdd.rdd.repartition(partitions.get)
    else vcRdd.rdd

    val genodata: RDD[(Array[Byte], List[(String, Array[Byte])])] = rdd.mapPartitions((iterator) => {
      val genotypebaos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val genotypeEncoder = EncoderFactory.get().binaryEncoder(genotypebaos, null)

      val genotypeDatumWriter: DatumWriter[Genotype] = new SpecificDatumWriter[Genotype](
        scala.reflect.classTag[Genotype]
          .runtimeClass
          .asInstanceOf[Class[Genotype]])

      val genotypesForHbase: Iterator[(Array[Byte], List[(String, Array[Byte])])] = iterator.map((putRecord) => {

        val myRowKey = KeyStrategy1.getKey(KeyStrategy1rowKeyInfo(putRecord.variant.variant.getContigName,
          putRecord.variant.variant.getStart.toInt,
          putRecord.variant.variant.getEnd.toInt,
          putRecord.variant.variant.getReferenceAllele,
          putRecord.variant.variant.getAlternateAllele))

        val genotypes: List[(String, Array[Byte])] = putRecord.genotypes.map((geno) => {
          genotypebaos.reset()
          genotypeDatumWriter.write(geno, genotypeEncoder)
          genotypeEncoder.flush()
          genotypebaos.flush()
          (geno.getSampleId, genotypebaos.toByteArray)
        }).toList

        (myRowKey, genotypes)
      })
      genotypesForHbase
    })

    val bulkLoadFlatMap = (t: (Array[Byte], List[(String, Array[Byte])])) => {
      val data = new ListBuffer[(KeyFamilyQualifier, Array[Byte])]
      val rowKey = t._1
      val family = Bytes.toBytes("g")
      t._2.foreach((x) => {
        val qualifier: String = x._1
        val value: Array[Byte] = x._2
        val mykeyFamilyQualifier = new KeyFamilyQualifier(rowKey, family, Bytes.toBytes(qualifier))
        data += Tuple2(mykeyFamilyQualifier, value)
      })

      data.iterator
    }

    dao.hbaseBulkLoad(genodata,
      hbaseTableName,
      bulkLoadFlatMap,
      stagingFolder)
  }

  /**
   *
   * @param dao HBase Data Access Object
   * @param hbaseTableName HBase table name
   * @param sampleIds List of sample ids
   * @param sampleListFile File containing one sample id per row
   * @param partitions number of paritions to use when performing bulk delete
   */

  def deleteGenotypeSamplesFromHBase(dao: HBaseSparkDAO,
                                     hbaseTableName: String,
                                     sampleIds: Option[List[String]] = None,
                                     sampleListFile: Option[String] = None,
                                     partitions: Option[Int] = Some(1)): Unit = {

    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    val sampleIdsFinal: List[String] =
      if (sampleListFile.isDefined) {
        val sampleIdsLst: List[String] = Source.fromFile(sampleListFile.get).getLines.toList
        sampleIdsLst.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIdsLst
      } else {
        sampleIds.get.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIds.get
      }

    val resultHBaseRDD: RDD[(ImmutableBytesWritable, Result)] =
      dao.hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

    dao.hbaseContext.bulkDelete[(ImmutableBytesWritable, Result)](resultHBaseRDD,
      TableName.valueOf(hbaseTableName),
      putRecord => {
        val currDelete = new Delete(putRecord._2.getRow)
        sampleIdsFinal.foreach((sample) => {
          currDelete.addColumns(Bytes.toBytes("g"), Bytes.toBytes(sample))
        })
        currDelete
      },
      partitions.get)
  }

  /**
   * Load VariantContextRDD from Genotype data in HBase
   *
   * @param dao HBase Data Access Object
   * @param hbaseTableName HBase table name
   * @param sampleIds List of sample ids
   * @param sequenceDictionaryId Id of sequenceDictionary that was saved along with genotype data to HBase
   * @param queryRegion Genomic ReferenceRegion used to limit query
   * @param partitions number of partitions in which to repartition RDD
   * @return
   */

  def loadGenotypesFromHBase(dao: HBaseSparkDAO,
                             hbaseTableName: String,
                             sampleIds: List[String],
                             sequenceDictionaryId: String,
                             queryRegion: Option[ReferenceRegion] = None, //one-based
                             partitions: Option[Int] = None): VariantContextRDD = {

    val sequenceDictionary = loadSequenceDictionaryFromHBase(dao, hbaseTableName + "_meta", sequenceDictionaryId)
    val sampleMetadata = loadSampleMetadataFromHBase(dao, hbaseTableName + "_meta", sampleIds)
    val genotypes = loadVariantContextsFromHBase(
      dao,
      hbaseTableName,
      Option(sampleIds),
      queryRegion = queryRegion,
      partitions = partitions)

    VariantContextRDD(genotypes, sequenceDictionary, sampleMetadata)

  }

}