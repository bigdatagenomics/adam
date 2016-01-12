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

import java.io.File
import java.util.UUID
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.PhredUtils._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName

case class TestSaveArgs(var outputPath: String) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var asSingleFile = false
  var blockSize = 128 * 1024 * 1024
  var pageSize = 1 * 1024 * 1024
  var compressionCodec = CompressionCodecName.GZIP
  var logLevel = "SEVERE"
  var disableDictionaryEncoding = false
}

class ADAMContextSuite extends ADAMFunSuite {

  sparkTest("sc.loadParquet should not fail on unmapped reads") {
    val readsFilepath = resourcePath("unmapped.sam")

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath)
    assert(bamReads.count === 200)
  }

  sparkTest("sc.loadParquet should not load a file without a type specified") {
    //load an existing file from the resources and save it as an ADAM file.
    //This way we are not dependent on the ADAM format (as we would if we used a pre-made ADAM file)
    //but we are dependent on the unmapped.sam file existing, maybe I should make a new one
    val readsFilepath = resourcePath("unmapped.sam")
    val bamReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath)
    //save it as an Adam file so we can test the Adam loader
    val bamReadsAdamFile = new File(Files.createTempDir(), "bamReads.adam")
    bamReads.adamParquetSave(bamReadsAdamFile.getAbsolutePath)
    intercept[IllegalArgumentException] {
      val noReturnType = sc.loadParquet(bamReadsAdamFile.getAbsolutePath)
    }
    //finally just make sure we did not break anything,we came might as well
    val returnType: RDD[AlignmentRecord] = sc.loadParquet(bamReadsAdamFile.getAbsolutePath)
    assert(manifest[returnType.type] != manifest[RDD[Nothing]])
  }

  sparkTest("can read a small .SAM file") {
    val path = resourcePath("small.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path)
    assert(reads.count() === 20)
  }

  sparkTest("can read a small .SAM with all attribute tag types") {
    val path = resourcePath("tags.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path)
    assert(reads.count() === 7)
  }

  sparkTest("can filter a .SAM file based on quality") {
    val path = resourcePath("small.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path)
      .filter(a => (a.getReadMapped && a.getMapq > 30))
    assert(reads.count() === 18)
  }

  test("Can convert to phred") {
    assert(successProbabilityToPhred(0.9) === 10)
    assert(successProbabilityToPhred(0.99999) === 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToSuccessProbability(10) > 0.89 && phredToSuccessProbability(10) < 0.91)
    assert(phredToSuccessProbability(50) > 0.99998 && phredToSuccessProbability(50) < 0.999999)
  }

  sparkTest("findFiles correctly finds a nested set of directories") {

    /**
     * Create the following directory structure, in the temp file location:
     *
     * .
     * |__ parent-dir/
     *     |__ subDir1/
     *     |   |__ match1/
     *     |   |__ match2/
     *     |__ subDir2/
     *     |   |__ match3/
     *     |   |__ nomatch4/
     *     |__ match5/
     *     |__ nomatch6/
     */

    val tempDir = File.createTempFile("ADAMContextSuite", "").getParentFile

    def createDir(dir: File, name: String): File = {
      val dirFile = new File(dir, name)
      dirFile.mkdir()
      dirFile
    }

    val parentName: String = "parent-" + UUID.randomUUID().toString
    val parentDir: File = createDir(tempDir, parentName)
    val subDir1: File = createDir(parentDir, "subDir1")
    val subDir2: File = createDir(parentDir, "subDir2")
    val match1: File = createDir(subDir1, "match1")
    val match2: File = createDir(subDir1, "match2")
    val match3: File = createDir(subDir2, "match3")
    val nomatch4: File = createDir(subDir2, "nomatch4")
    val match5: File = createDir(parentDir, "match5")
    val nomatch6: File = createDir(parentDir, "nomatch6")

    /**
     * Now, run findFiles() on the parentDir, and make sure we find match{1, 2, 3, 5} and _do not_
     * find nomatch{4, 6}
     */

    val paths = sc.findFiles(new Path(parentDir.getAbsolutePath), "^match.*")

    assert(paths.size === 4)

    val pathNames = paths.map(_.getName)
    assert(pathNames.contains("match1"))
    assert(pathNames.contains("match2"))
    assert(pathNames.contains("match3"))
    assert(pathNames.contains("match5"))
  }

  sparkTest("loadADAMFromPaths can load simple RDDs that have just been saved") {
    val contig = Contig.newBuilder
      .setContigName("abc")
      .setContigLength(1000000)
      .setReferenceURL("http://abc")
      .build

    val a0 = AlignmentRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setContig(contig)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = AlignmentRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()

    val saved = sc.parallelize(Seq(a0, a1))
    val loc = tempLocation()
    val path = new Path(loc)

    saved.saveAsParquet(TestSaveArgs(loc),
      new SequenceDictionary(Vector(SequenceRecord.fromADAMContig(contig))),
      RecordGroupDictionary.empty)
    try {
      val loaded = sc.loadAlignmentsFromPaths(Seq(path))

      assert(loaded.count() === saved.count())
    } catch {
      case (e: Exception) =>
        println(e)
        throw e
    }
  }

  /*
   Little helper function -- because apparently createTempFile creates an actual file, not
   just a name?  Whereas, this returns the name of something that could be mkdir'ed, in the
   same location as createTempFile() uses, so therefore the returned path from this method
   should be suitable for adamParquetSave().
   */
  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("ADAMContextSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("Can read a .gtf file") {
    val path = testFile("features/Homo_sapiens.GRCh37.75.trun20.gtf")
    val features: RDD[Feature] = sc.loadFeatures(path)
    assert(features.count === 15)
  }

  sparkTest("Can read a .bed file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("features/gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.loadFeatures(path)
    assert(features.count === 10)
  }

  sparkTest("Can read a .narrowPeak file") {
    val path = testFile("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val annot: RDD[Feature] = sc.loadFeatures(path)
    assert(annot.count === 10)
  }

  sparkTest("Can read a .interval_list file") {
    val path = testFile("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val annot: RDD[Feature] = sc.loadFeatures(path)
    assert(annot.count == 369)
    val arr = annot.collect

    val first = arr.find(f => f.getContig.getContigName == "chr1" && f.getStart == 14415L && f.getEnd == 14498L).get
    assert(first.getContig.getContigLength == 249250621L)
    assert(first.getContig.getReferenceURL == "file:/gs01/projects/ngs/resources/gatk/2.3/ucsc.hg19.parmasked.fasta")
    assert(first.getContig.getContigMD5 == "1b22b98cdeb4a9304cb5d48026a85128")
    assert(
      first
        .getDbxrefs
        .map(dbxref => dbxref.getDb -> dbxref.getAccession)
        .groupBy(_._1)
        .mapValues(_.map(_._2).toSet) ==
        Map(
          "gn" -> Set("DDX11L1", "RP11-34P13.2"),
          "ens" -> Set("ENSG00000223972", "ENSG00000227232"),
          "vega" -> Set("OTTHUMG00000000958", "OTTHUMG00000000961")
        )
    )

    val last = arr.find(f => f.getContig.getContigName == "chrY" && f.getStart == 27190031L && f.getEnd == 27190209L).get
    assert(last.getContig.getContigLength == 59373566L)
    assert(last.getContig.getReferenceURL == "file:/gs01/projects/ngs/resources/gatk/2.3/ucsc.hg19.parmasked.fasta")
    assert(last.getContig.getContigMD5 == "3393b0779f142dc59f4cfcc22b61c1ee")
    assert(
      last
        .getDbxrefs
        .map(dbxref => dbxref.getDb -> dbxref.getAccession)
        .groupBy(_._1)
        .mapValues(_.map(_._2).toSet) ==
        Map(
          "gn" -> Set("BPY2C"),
          "ccds" -> Set("CCDS44030"),
          "ens" -> Set("ENSG00000185894"),
          "vega" -> Set("OTTHUMG00000045199")
        )
    )
  }

  sparkTest("can read a small .vcf file") {
    val path = resourcePath("small.vcf")

    val vcs = sc.loadGenotypes(path).toVariantContext.collect.sortBy(_.position)
    assert(vcs.size === 5)

    val vc = vcs.head
    assert(vc.genotypes.size === 3)

    val gt = vc.genotypes.head
    assert(gt.getVariantCallingAnnotations != null)
    assert(gt.getReadDepth === 20)
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.ifq".format(testNumber)
    val path = ClassLoader.getSystemClassLoader.getResource(inputName).getFile

    sparkTest("import records from interleaved FASTQ: %d".format(testNumber)) {

      val reads = sc.loadAlignments(path)
      if (testNumber == 1) {
        assert(reads.count === 6)
        assert(reads.filter(_.getReadPaired).count === 6)
        assert(reads.filter(_.getReadInFragment == 0).count === 3)
        assert(reads.filter(_.getReadInFragment == 1).count === 3)
      } else {
        assert(reads.count === 4)
        assert(reads.filter(_.getReadPaired).count === 4)
        assert(reads.filter(_.getReadInFragment == 0).count === 2)
        assert(reads.filter(_.getReadInFragment == 1).count === 2)
      }

      assert(reads.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.collect.forall(_.getQual.toString.length === 250))
    }
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "fastq_sample%d.fq".format(testNumber)
    val path = ClassLoader.getSystemClassLoader.getResource(inputName).getFile

    sparkTest("import records from single ended FASTQ: %d".format(testNumber)) {

      val reads = sc.loadAlignments(path)
      if (testNumber == 1) {
        assert(reads.count === 6)
        assert(reads.filter(_.getReadPaired).count === 0)
      } else if (testNumber == 4) {
        assert(reads.count === 4)
        assert(reads.filter(_.getReadPaired).count === 0)
      } else {
        assert(reads.count === 5)
        assert(reads.filter(_.getReadPaired).count === 0)
      }

      assert(reads.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.collect.forall(_.getQual.toString.length === 250))
    }
  }

  sparkTest("filter on load using the filter2 API") {
    val path = resourcePath("bqsr1.vcf")

    val variants: RDD[Variant] = sc.loadVariants(path)
    assert(variants.count === 681)

    val loc = tempLocation()
    variants.adamParquetSave(loc, 1024, 1024) // force more than one row group (block)

    val pred: FilterPredicate = (LongColumn("start") === 16097631L)
    // the following only reads one row group
    val adamVariants: RDD[Variant] = sc.loadParquetVariants(loc, predicate = Some(pred))
    assert(adamVariants.count === 1)
  }
}

