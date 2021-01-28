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
package org.bdgenomics.adam.util

import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.formats.avro.Alignment
import org.seqdoop.hadoop_bam.CRAMInputFormat

class ParallelFileMergerSuite extends ADAMFunSuite {

  sparkTest("cannot write both empty gzip block and cram eof") {
    intercept[IllegalArgumentException] {
      // we don't need to pass real paths here
      ParallelFileMerger.mergeFiles(sc,
        new Path("output"),
        new Path("head"),
        writeEmptyGzipBlock = true,
        writeCramEOF = true)
    }
  }

  sparkTest("buffer size must be non-negative") {
    intercept[IllegalArgumentException] {
      // we don't need to pass real paths here
      ParallelFileMerger.mergeFiles(sc,
        new Path("output"),
        new Path("head"),
        optBufferSize = Some(0))
    }
  }

  sparkTest("get the size of several files") {
    val files = Seq(testFile("unmapped.sam"),
      testFile("small.sam"))
      .map(new Path(_))
    val fileSizes = Seq(29408, 3189)
    val filesWithSizes = files.zip(fileSizes)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val (size, sizes) = ParallelFileMerger.getFullSize(fs, files)

    assert(size === fileSizes.sum.toLong)
    sizes.map(_._2)
      .zip(fileSizes)
      .foreach(p => assert(p._1 === p._2))
  }

  sparkTest("block size must be positive and non-zero when trying to merge files") {
    intercept[AssertionError] {
      ParallelFileMerger.generateMerges(0, Seq((new Path(testFile("small.sam")), 3093L)))
    }
  }

  sparkTest("must provide files to merge") {
    intercept[AssertionError] {
      ParallelFileMerger.generateMerges(1024, Seq.empty)
    }
  }

  sparkTest("if two files are both below the block size, they should merge into one shard") {
    val files = Seq(testFile("unmapped.sam"),
      testFile("small.sam"))
      .map(new Path(_))

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileSizesMap = files.map(f => (f, fs.getFileStatus(f).getLen().toInt))
      .toMap

    val (_, filesWithSizes) = ParallelFileMerger.getFullSize(fs, files)
    val merges = ParallelFileMerger.generateMerges(Int.MaxValue,
      filesWithSizes)
    assert(merges.size === 1)
    val (index, paths) = merges.head
    assert(index === 0)
    assert(paths.size === 2)
    paths.foreach(t => {
      val (file, start, end) = t
      val path = new Path(file)
      assert(start === 0)
      assert(fileSizesMap.contains(path))

      val fileSize = fileSizesMap(path)
      assert(end === fileSize - 1)
    })
  }

  sparkTest("merge two files where one is greater than the block size") {

    // unmapped.sam -> slightly under 29k
    // small.sam -> 3k
    val files = Seq(testFile("unmapped.sam"),
      testFile("small.sam"))
      .map(new Path(_))

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileSizesMap = files.map(f => (f, fs.getFileStatus(f).getLen().toInt))
      .toMap

    val (_, filesWithSizes) = ParallelFileMerger.getFullSize(fs, files)
    val merges = ParallelFileMerger.generateMerges(16 * 1024, // 16KB
      filesWithSizes)
    assert(merges.size === 2)
    val optFirstMerge = merges.filter(_._1 == 0)
      .headOption
    assert(optFirstMerge.isDefined)
    optFirstMerge.foreach(firstMerge => {
      val (_, merges) = firstMerge
      assert(merges.size === 1)
      val (file, start, end) = merges.head
      val path = new Path(file)
      assert(path.getName === "unmapped.sam")
      assert(start === 0)
      assert(end === 16 * 1024 - 1)
    })
    val optSecondMerge = merges.filter(_._1 == 1)
      .headOption
    assert(optSecondMerge.isDefined)
    optSecondMerge.foreach(firstMerge => {
      val (_, merges) = firstMerge
      assert(merges.size === 2)
      val (file0, start0, end0) = merges.head
      val path0 = new Path(file0)
      assert(path0.getName === "unmapped.sam")
      assert(start0 === 16 * 1024)
      assert(end0 === (fs.getFileStatus(path0).getLen().toInt - 1))
      val (file1, start1, end1) = merges.tail.head
      val path1 = new Path(file1)
      assert(path1.getName === "small.sam")
      assert(start1 === 0)
      assert(end1 === (fs.getFileStatus(path1).getLen().toInt - 1))
    })
  }

  sparkTest("merge a sharded sam file") {
    val reads = sc.loadAlignments(testFile("unmapped.sam"))
    val outPath = tmpFile("out.sam")

    reads.transform((rdd: RDD[Alignment]) => rdd.repartition(4))
      .saveAsSam(outPath, asSingleFile = true, deferMerging = true)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val filesToMerge = (Seq(outPath + "_head") ++ (0 until 4).map(i => {
      outPath + "_tail/part-r-0000%d.sam".format(i)
    })).map(new Path(_))
      .map(p => (p.toString, 0L, fs.getFileStatus(p).getLen().toLong - 1L))

    ParallelFileMerger.mergePaths(outPath,
      filesToMerge,
      sc.broadcast(sc.hadoopConfiguration),
      false,
      false)

    val mergedReads = sc.loadAlignments(outPath)

    assert(mergedReads.rdd.count === reads.rdd.count)
  }

  sparkTest("merge a sharded bam file") {
    val reads = sc.loadAlignments(testFile("unmapped.sam"))
    val outPath = tmpFile("out.bam")

    reads.transform((rdd: RDD[Alignment]) => rdd.repartition(4))
      .saveAsSam(outPath, asSingleFile = true, deferMerging = true)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val filesToMerge = (Seq(outPath + "_head") ++ (0 until 4).map(i => {
      outPath + "_tail/part-r-0000%d.bam".format(i)
    })).map(new Path(_))
      .map(p => (p.toString, 0L, fs.getFileStatus(p).getLen().toLong - 1L))

    ParallelFileMerger.mergePaths(outPath,
      filesToMerge,
      sc.broadcast(sc.hadoopConfiguration),
      true,
      false)

    val mergedReads = sc.loadAlignments(outPath)

    assert(mergedReads.rdd.count === reads.rdd.count)
  }

  sparkTest("merge a sharded cram file") {
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)
    val reads = sc.loadAlignments(testFile("artificial.cram"))
    val outPath = tmpFile("out.cram")

    reads.transform((rdd: RDD[Alignment]) => rdd.repartition(4))
      .saveAsSam(outPath, isSorted = true, asSingleFile = true, deferMerging = true)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val filesToMerge = (Seq(outPath + "_head") ++ (0 until 4).map(i => {
      outPath + "_tail/part-r-0000%d.cram".format(i)
    })).map(new Path(_))
      .map(p => (p.toString, 0L, fs.getFileStatus(p).getLen().toLong - 1L))

    ParallelFileMerger.mergePaths(outPath,
      filesToMerge,
      sc.broadcast(sc.hadoopConfiguration),
      false,
      true)

    val mergedReads = sc.loadAlignments(outPath)

    assert(mergedReads.rdd.count === reads.rdd.count)
  }

  test("can't turn a negative index into a path") {
    intercept[AssertionError] {
      ParallelFileMerger.indexToPath(-1, "nonsense")
    }
  }

  test("generate a path from an index") {
    val path = ParallelFileMerger.indexToPath(2, "nonsense")
    assert(path.toString === "nonsense_part-r-00002")
  }
}
