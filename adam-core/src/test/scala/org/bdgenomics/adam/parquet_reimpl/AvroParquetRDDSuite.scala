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
package org.bdgenomics.adam.parquet_reimpl

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, File }
import java.lang.Iterable
import java.net.URI

import org.bdgenomics.adam.io._
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.parquet_reimpl.filters.{ FilterTuple, SerializableUnboundRecordFilter }
import org.bdgenomics.adam.parquet_reimpl.index.ReferenceFoldingContext._
import org.bdgenomics.adam.parquet_reimpl.index._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.util._
import org.bdgenomics.formats.avro.{ ADAMRecord, ADAMFlatGenotype }
import parquet.column.ColumnReader
import parquet.filter.{ RecordFilter, UnboundRecordFilter }

import scala.collection.JavaConversions._
import scala.io.Source

class RDDFunSuite extends SparkFunSuite {

  def resourceFile(resourceName: String): File = {
    val path = Thread.currentThread().getContextClassLoader.getResource(resourceName).getFile
    new File(path)
  }

  def resourceLocator(resourceName: String): FileLocator =
    new LocalFileLocator(resourceFile(resourceName))
}

class AvroIndexedParquetRDDSuite extends RDDFunSuite {

  def writeIndexAsBytes(rootLocator: FileLocator, parquets: String*): FileLocator = {
    val rangeIndexGenerator = new IDRangeIndexGenerator[ADAMFlatGenotype](
      (v: ADAMFlatGenotype) => v.getSampleId.toString)
    val debugPrint = false

    val entries = parquets.flatMap {
      case parquet: String =>
        rangeIndexGenerator.addParquetFile(rootLocator, parquet)
    }

    val rangeIndex = new IDRangeIndex(entries)

    val byteArrayOutputStream = new ByteArrayOutputStream()
    val indexWriter = new IDRangeIndexWriter(byteArrayOutputStream)

    if (debugPrint) {
      println("Writing entries: ")
      rangeIndex.entries.foreach(entry => println(entry.line))
    }

    rangeIndex.entries.foreach(indexWriter.write)
    indexWriter.close()

    if (debugPrint) {
      println("Wrote lines: ")
      val is = new ByteArrayInputStream(byteArrayOutputStream.toByteArray)
      val lines = Source.fromInputStream(is).getLines()
      lines.foreach(println)
    }

    new ByteArrayLocator(byteArrayOutputStream.toByteArray)
  }

  sparkTest("S3AvroIndexedParquetRDD can read a local index and produce the correct records") {

    val inputDataFile = resourceFile("small_adam.fgenotype")
    val inputDataRootLocator = new LocalFileLocator(inputDataFile.getParentFile)
    val indexFileLocator = writeIndexAsBytes(inputDataRootLocator, "small_adam.fgenotype")

    val queryRange = ReferenceRegion("chr1", 5000, 15000)
    val filter = new FilterTuple[ADAMFlatGenotype, IDRangeIndexEntry](null, null,
      new IDRangeIndexPredicate(queryRange))
    val indexedRDD = new AvroIndexedParquetRDD[ADAMFlatGenotype](sc, filter, indexFileLocator, inputDataRootLocator, None)

    val fileMetadata = AvroParquetFileMetadata(new LocalFileLocator(inputDataFile), None)
    val records = indexedRDD.compute(fileMetadata.partition(0), null).toSeq

    assert(records.size === 15)
    assert(records.map(_.getReferenceName).distinct === Seq("chr1"))
  }

  sparkTest("S3AvroIndexedParquetRDD produces no partitions, if the query overlaps no read groups") {

    val inputDataFile = resourceFile("small_adam.fgenotype")
    val inputDataRootLocator = new LocalFileLocator(inputDataFile.getParentFile)
    val indexFileLocator = writeIndexAsBytes(inputDataRootLocator, "small_adam.fgenotype")

    // this query Range is on chr10, which should overlap no read groups at all
    val queryRange = ReferenceRegion("chr10", 5000, 15000)
    val filter = new FilterTuple[ADAMFlatGenotype, IDRangeIndexEntry](null, null,
      new IDRangeIndexPredicate(Some(queryRange)))
    val indexedRDD = new AvroIndexedParquetRDD[ADAMFlatGenotype](sc, filter, indexFileLocator, inputDataRootLocator, None)

    assert(indexedRDD.partitions.length === 0)
  }

  sparkTest("produces 1 partition when the filter only overlaps one partition") {

    val resource = "jc_adam.fgenotype"
    val inputDataFile = resourceFile(resource)
    val inputDataRootLocator = new LocalFileLocator(inputDataFile.getParentFile)
    val indexFileLocator = writeIndexAsBytes(inputDataRootLocator, resource)

    val queryRange = ReferenceRegion("1", 60000, 70000)
    val rangePredicate = new IDRangeIndexPredicate(Some(queryRange), Some(Set[String]("1e54a67a-e285-4764-954f-83f23c049701")))
    val filter = new FilterTuple[ADAMFlatGenotype, IDRangeIndexEntry](
      null, null, rangePredicate)

    val rangeIndex = new IDRangeIndex(indexFileLocator)
    assert(rangeIndex.findIndexEntries(rangePredicate).toSeq.length === 1)

    val indexedRDD = new AvroIndexedParquetRDD[ADAMFlatGenotype](sc, filter, indexFileLocator, inputDataRootLocator, None)
    assert(indexedRDD.partitions.length === 1)

    val records = indexedRDD.collect()
    assert(records.length === 390)
  }

  sparkTest("indexing produces the same records as the non-indexed RDD") {

    val resource = "jc_adam.fgenotype"
    val inputDataFile = resourceFile(resource)
    val inputDataLocator = new LocalFileLocator(inputDataFile)
    val inputDataRootLocator = new LocalFileLocator(inputDataFile.getParentFile)
    val indexFileLocator = writeIndexAsBytes(inputDataRootLocator, resource)

    val queryRange = ReferenceRegion("1", 60000, 70000)
    val rangePredicate = new IDRangeIndexPredicate(
      Some(queryRange),
      Some(Set[String]("1e54a67a-e285-4764-954f-83f23c049701")))

    val filter = new FilterTuple[ADAMFlatGenotype, IDRangeIndexEntry](
      RDDFunSuite.createRangeFilter(queryRange), null, rangePredicate)

    val rangeIndex = new IDRangeIndex(indexFileLocator)
    assert(rangeIndex.findIndexEntries(rangePredicate).toSeq.length === 1)

    val indexedRDD = new AvroIndexedParquetRDD[ADAMFlatGenotype](sc, filter, indexFileLocator, inputDataRootLocator, None)
    assert(indexedRDD.partitions.length === 1)

    val nonIndexedRDD = new AvroParquetRDD[ADAMFlatGenotype](sc, filter.recordFilter, inputDataLocator, None)
    assert(indexedRDD.count() === nonIndexedRDD.count())

  }
}

object RDDFunSuite {

  def descriptorMatches(name: String)(d: ColumnReader): Boolean = {
    val path = d.getDescriptor.getPath
    path(path.length - 1) == name
  }

  def createRangeFilter(r: ReferenceRegion): SerializableUnboundRecordFilter =
    new SerializableUnboundRecordFilter() {
      override def bind(readers: Iterable[ColumnReader]): RecordFilter =
        new RecordFilter {
          override def isMatch: Boolean = {
            readers.find(descriptorMatches("referenceName")).get.getBinary.toStringUsingUTF8 == r.referenceName &&
              readers.find(descriptorMatches("position")).get.getLong >= r.start &&
              readers.find(descriptorMatches("position")).get.getLong < r.end
          }
        }
    }

}

class AvroParquetRDDSuite extends SparkFunSuite {

  lazy val credentials = new CredentialsProperties(new File(System.getProperty("user.home") + "/spark.conf"))
    .awsCredentials(Some("s3"))

  lazy val bucketName = System.getenv("BUCKET_NAME")
  lazy val parquetLocation = System.getenv("PARQUET_LOCATION")

  sparkTest("Retrieve records from a Parquet file through HTTP", silenceSpark = true, NetworkConnected) {
    val locator = new HTTPFileLocator(URI.create("http://www.cs.berkeley.edu/~massie/adams/part1"))
    val rdd = new AvroParquetRDD[ADAMRecord](
      sc,
      null,
      locator,
      None)

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.count() === 49)
  }

  sparkTest("Retrieve records from a Parquet file through S3", silenceSpark = true, NetworkConnected, S3Test) {

    val locator = new S3FileLocator(credentials, bucketName, parquetLocation)
    val rdd = new AvroParquetRDD[ADAMRecord](
      sc,
      null,
      locator,
      None)

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.count() === 49)
  }

  sparkTest("Using a projection works with HTTP", silenceSpark = true, NetworkConnected) {

    import org.bdgenomics.adam.projections.ADAMRecordField._

    val schema = Projection(readName, start, contig)

    val locator = new HTTPFileLocator(URI.create("http://www.cs.berkeley.edu/~massie/adams/part1"))
    val rdd = new AvroParquetRDD[ADAMRecord](
      sc,
      null,
      locator,
      Some(schema))

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.count() === 49)
  }

  sparkTest("Using a projection works with S3", silenceSpark = true, NetworkConnected, S3Test) {

    import org.bdgenomics.adam.projections.ADAMRecordField._

    val schema = Projection(readName, start, contig)

    val locator = new S3FileLocator(credentials, bucketName, parquetLocation)
    val rdd = new AvroParquetRDD[ADAMRecord](
      sc,
      null,
      locator,
      Some(schema))

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.count() === 49)
  }

  sparkTest("Using a filter works with HTTP", silenceSpark = true, NetworkConnected) {

    import org.bdgenomics.adam.projections.ADAMRecordField._

    val schema = Projection(readName, start, sequence)
    val filter = new ReadNameFilter("simread:1:189606653:true")

    val locator = new HTTPFileLocator(URI.create("http://www.cs.berkeley.edu/~massie/adams/part1"))
    val rdd = new AvroParquetRDD[ADAMRecord](
      sc,
      filter,
      locator,
      Some(schema))

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.collect().length === 1)
    assert(rdd.count() === 1)
  }

  sparkTest("Using a filter works with S3", silenceSpark = true, NetworkConnected, S3Test) {

    import org.bdgenomics.adam.projections.ADAMRecordField._

    val schema = Projection(readName, start, sequence)
    val filter = new ReadNameFilter("simread:1:189606653:true")

    val locator = new S3FileLocator(credentials, bucketName, parquetLocation)
    val rdd = new AvroParquetRDD[ADAMRecord](
      sc,
      filter,
      locator,
      Some(schema))

    val value = rdd.first()
    assert(value != null)
    assert(value.getReadName === "simread:1:189606653:true")
    assert(value.getStart === 189606653L)

    assert(rdd.collect().length === 1)
    assert(rdd.count() === 1)
  }

}

class ReadNameFilter(value: String) extends UnboundRecordFilter with Serializable {

  def bind(readers: Iterable[ColumnReader]): RecordFilter = {
    val reader = readers.find(_.getDescriptor.getPath.last == "readName").get
    new RecordFilter() {
      def isMatch: Boolean = {
        val mtch = reader.getBinary.toStringUsingUTF8 == value
        mtch
      }
    }
  }
}

