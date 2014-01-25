/*
 * Copyright (c) 2013. The Broad Institute
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

import scala.util.Random
import edu.berkeley.cs.amplab.adam.rdd.recalibration._
import scala.collection.mutable
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._

class RecalibrateBaseQualitiesSuite extends SparkFunSuite {
  val RANDOM = new Random("RecalibrationSuite".hashCode)

  def newBaseCovar(isMM: Boolean, isMask: Boolean): BaseCovariates = {
    new BaseCovariates(RANDOM.nextInt(200), (1 to 5).map(t => RANDOM.nextInt(1000)).toArray, RANDOM.nextInt(60).toByte, isMM, isMask)
  }

  test("Util :: Covariates :: BaseCovariates :: Creation") {
    // BaseCovariates is just a holder class, so there's not much to test: creation
    val covar = new BaseCovariates(127, (1 to 21).toArray, 33.toByte, true, false)
    assert(covar.qual == 33.toByte)
    assert(covar.isMismatch)
    assert(covar.covar.deep == (1 to 21).toArray.deep)
  }

  test("Util :: RecalTable :: ErrorCount :: Creation") {
    val errorCount = new ErrorCount()
    assert(errorCount.basesObserved == 0L)
    assert(errorCount.mismatches == 0L)
    assert(errorCount.getErrorProb.isEmpty)
  }

  test("Util :: RecalTable :: ErrorCount :: +=") {
    for (numBaseCovar <- (1 to 50).map(_ * 100)) {
      var unFilteredMismatches = 0L
      var unFilteredSites = 0L
      val count = new ErrorCount
      for (i <- 1 to numBaseCovar) {
        val isMM = RANDOM.nextBoolean()
        val isMask = RANDOM.nextBoolean()
        if (!isMask) {
          unFilteredSites += 1
        }
        if (isMM && !isMask) {
          unFilteredMismatches += 1
        }
        count += newBaseCovar(isMM, isMask)
      }
      assert(count.basesObserved == unFilteredSites)
      assert(count.mismatches == unFilteredMismatches)
      assert(count.getErrorProb.get == unFilteredMismatches.toDouble / unFilteredSites)
    }
  }

  test("Util :: RecalTable :: ErrorCount :: ++") {
    // symmetry testing
    def _test(v1: ErrorCount, v2: ErrorCount, baseSum: Int, mismatchSum: Int) = {
      val left = v1 ++ v2
      val right = v2 ++ v1
      assert(left.basesObserved == baseSum)
      assert(right.basesObserved == baseSum)
      assert(left.mismatches == mismatchSum)
      assert(right.mismatches == mismatchSum)
      assert(left.getErrorProb.isEmpty == right.getErrorProb.isEmpty)
      if (!left.getErrorProb.isEmpty) {
        assert(left.getErrorProb.get == math.max(1e-6, mismatchSum.toDouble / baseSum))
        assert(right.getErrorProb.get == math.max(1e-6, mismatchSum.toDouble / baseSum))
      }
    }
    val baseSums = List(10000, 100000, 1000000, 10000000)
    val mismatchSums = List(0, 1, 10, 100, 500, 1000, 5000, 10000)
    val numReplicates = 50
    for (sums <- baseSums zip mismatchSums) {
      for (repl <- 1 to numReplicates) {
        val bases1 = RANDOM.nextInt(sums._1)
        val bases2 = sums._1 - bases1
        val mismatch1 = if (sums._2 > 0) math.min(bases1, RANDOM.nextInt(sums._2)) else 0
        val mismatch2 = sums._2 - mismatch1
        val ec1 = new ErrorCount
        ec1.basesObserved += bases1
        ec1.mismatches += mismatch1
        val ec2 = new ErrorCount
        ec2.basesObserved += bases2
        ec2.mismatches += mismatch2
        _test(ec1, ec2, sums._1, sums._2)
      }
    }
  }

  test("Util :: RecalTable :: ErrorCounts :: Construction") {
    val errCounts = new ErrorCounts
    assert(errCounts.errorsByVariate.toList.toArray.deep == Map[Int, ErrorCount]().toList.toArray.deep)
  }

  test("Util :: RecalTable :: ErrorCounts :: Update") {
    // the eror counts within RecalTable get updated by the fragment
    // counts(covarValue) += baseCovars
    for (maxCovariateValue <- List(3, 10, 20, 40)) {
      for (numObservationsPer <- List(100, 200, 500, 1000, 10000)) {
        var errors: Map[Int, Int] = Map[Int, Int]()
        val errorCounts = new ErrorCounts
        for (covar <- 1 to maxCovariateValue) {
          for (obs <- 1 to numObservationsPer) {
            val isError = RANDOM.nextBoolean()
            if (isError)
              errors += (covar -> (1 + errors.getOrElse(covar, 0)))
            errorCounts(covar) += newBaseCovar(isError, false)
          }
        }
        for (c <- errors.keys) {
          assert(errors(c) == errorCounts(c).mismatches)
          assert(errorCounts(c).basesObserved == numObservationsPer)
        }
      }
    }
  }

  test("Util :: RecalTable :: ErrorCounts :: ++") {
    def _errorCount(obs: Int, err: Int): ErrorCount = {
      val _err = new ErrorCount
      _err.basesObserved += obs
      _err.mismatches += err
      _err
    }
    val maxCovariateValue = List(3, 8, 15, 140)
    val num_replicates = 50
    for (maxCov <- maxCovariateValue) {
      for (repl <- 1 to num_replicates) {
        val obs1 = (1 to maxCov).map(t => (t, RANDOM.nextInt(200), 200 + RANDOM.nextInt(25000)))
        val obs2 = (1 to maxCov).map(t => (t, RANDOM.nextInt(200), 200 + RANDOM.nextInt(25000)))
        // wrap these into error counts
        val errMap1 = obs1.map(t => (t._1, _errorCount(t._3, t._2))).toMap
        val errMap2 = obs2.map(t => (t._1, _errorCount(t._3, t._2))).toMap
        val counts1 = new ErrorCounts
        counts1.errorsByVariate = mutable.HashMap[Int, ErrorCount]() ++ errMap1
        val counts2 = new ErrorCounts
        counts2.errorsByVariate = mutable.HashMap[Int, ErrorCount]() ++ errMap2
        val left = counts1 ++ counts2
        val right = counts2 ++ counts1
        for (t <- errMap1.keys) {
          assert(left(t).basesObserved == errMap1(t).basesObserved + errMap2(t).basesObserved)
          assert(right(t).basesObserved == errMap1(t).basesObserved + errMap2(t).basesObserved)
          assert(left(t).mismatches == errMap1(t).mismatches + errMap2(t).mismatches)
          assert(right(t).mismatches == errMap1(t).mismatches + errMap2(t).mismatches)
        }
      }
    }
  }

  test("Util :: RecalTable :: Construction") {
    val table = new RecalTable()
    assert(table.counts.toList.toArray.deep == Map[Int, Array[ErrorCounts]]().toList.toArray.deep)
  }

  def cartesian(limits: Array[Int], offset: Int = 0): Array[Array[Int]] = {
    val thisWrapped = Array(limits)
    val rest = Range(offset, limits.size).filter(t => limits(t) > 0).flatMap(t => {
      val m = limits.clone()
      m(t) -= 1
      cartesian(m, t)
    }).toArray
    (thisWrapped ++ rest).toList.toArray
  }

  def cartesianLim(maxV: Array[Int], minV: Array[Int], offset: Int = 0): Array[Array[Int]] = {
    val thisWrapped = Array(maxV)
    val rest = Range(offset, maxV.size).filter(t => maxV(t) > minV(t)).flatMap(t => {
      val m = maxV.clone()
      m(t) -= 1
      cartesianLim(m, minV, t)
    }).toArray
    (thisWrapped ++ rest).toList.toArray
  }

  test("Util :: RecalTable :: +=") {
    val qualByRG_max = List(60, 120, 240, 300)
    val covariate_max = List(List(5, 3, 7), List(2, 12, 10, 4), List(100, 100), List(10))
    var combiTestNo = 0
    for (maxQRG <- qualByRG_max) {
      for (covarMaxima <- covariate_max) {
        combiTestNo += 1
        val numAdditions = maxQRG * covarMaxima.foldLeft(1)((a, b) => a * (b + 1))
        System.out.println("Running combinatorial test %d, with %d elements".format(combiTestNo, numAdditions))
        val covariateArray = cartesian(covarMaxima.toArray)
        val timeStart = System.nanoTime()
        val table = new RecalTable()
        for (qual <- 1 to maxQRG) {
          for (covars <- covariateArray) {
            table += new BaseCovariates(qual, covars, RANDOM.nextInt(60).toByte, true, false)
            table += new BaseCovariates(qual, covars, RANDOM.nextInt(60).toByte, false, false)
          }
        }
        for (qual <- 1 to maxQRG) {
          for (covarIndex <- Range(0, covarMaxima.size)) {
            val errCounts = table.lookup(qual, covarMaxima.size)
            val numTimesSeen = Range(0, covarMaxima.size).filter(_ != covarIndex).foldLeft(1)((a, b) => a * (covarMaxima(b) + 1))
            assert(errCounts(covarIndex)(1).basesObserved == 2 * numTimesSeen)
            assert(errCounts(covarIndex)(0).mismatches == numTimesSeen)
          }
        }
        val timeStop = System.nanoTime()
        System.out.println("Test took %f seconds".format((timeStop - timeStart).toDouble / math.pow(10.0, 9)))
      }
    }
  }

  test("Util :: RecalTable :: +") {
    // test must wait until StandardCovariate objects can be instantiated
  }

  def _makeTable(minQ: Int, maxQ: Int, minCov: List[Int], maxCov: List[Int]): RecalTable = {
    val errMap = mutable.HashMap[Int, Array[ErrorCounts]]()
    var expMM = 0.0
    errMap ++= (minQ to maxQ).map(qual => {
      val counts = maxCov.map(t => new ErrorCounts).toArray
      for (covars <- cartesianLim(maxCov.toArray, minCov.toArray)) {
        for (covarIdx <- Range(0, covars.size)) {
          val bc = newBaseCovar(isMM = true, isMask = false)
          counts(covarIdx)(covars(covarIdx)) += bc
          expMM += RecalUtil.qualToErrorProb(bc.qual)
        }
      }
      (qual, counts)
    })
    new RecalTable(errMap, expMM)
  }

  test("Util :: RecalTable :: ++ :: disjoint") {
    // test entirely disjoint
    val table1 = _makeTable(1, 30, List(0, 0, 0), List(2, 2, 2))
    val table2 = _makeTable(31, 60, List(3, 3, 3), List(5, 5, 5))
    val left = table1 ++ table2
    val right = table2 ++ table1
    assert(left.counts.size == 60)
    assert(right.counts.size == 60)
    left.counts.values.foreach(a => {
      assert(a.size == 3)
      a.foreach(b => {
        assert(b.errorsByVariate.size == 3)
        b.errorsByVariate.values.foreach(c => {
          assert(c.basesObserved == 9)
          assert(c.mismatches == 9)
        })
      })
    })
  }

  test("Util :: RecalTable :: ++ :: quals overlap") {
    // test overlapping quals only
    val table1 = _makeTable(1, 50, List(0, 0, 0), List(2, 2, 2))
    val table2 = _makeTable(31, 80, List(3, 3, 3), List(5, 5, 5))
    val left = table1 ++ table2
    val right = table2 ++ table1
    assert(left.counts.size == 80)
    assert(right.counts.size == 80)
    left.counts.values.foreach(a => {
      assert(a.size == 3)
      a.foreach(b => {
        assert(b.errorsByVariate.size <= 6)
        b.errorsByVariate.values.foreach(c => {
          assert(c.basesObserved == 9)
          assert(c.mismatches == 9)
        })
      })
    })
  }

  test("Util :: RecalTable :: ++ :: covars overlap") {
    // test overlapping quals only
    val table1 = _makeTable(1, 30, List(0, 0, 0), List(2, 2, 2))
    val table2 = _makeTable(31, 60, List(1, 1, 1), List(3, 3, 3))
    val left = table1 ++ table2
    val right = table2 ++ table1
    assert(left.counts.size == 60)
    assert(right.counts.size == 60)
    left.counts.values.foreach(a => {
      assert(a.size == 3)
      a.foreach(b => {
        assert(b.errorsByVariate.size == 3)
        b.errorsByVariate.values.foreach(c => {
          assert(c.basesObserved == 9)
          assert(c.mismatches == 9)
        })
      })
    })
  }

  test("Util :: RecalTable :: ++ :: everything overlaps") {
    // test overlapping quals only
    val table1 = _makeTable(1, 50, List(0, 0, 0), List(2, 2, 2))
    val table2 = _makeTable(1, 50, List(0, 0, 0), List(2, 2, 2))
    val left = table1 ++ table2
    val right = table2 ++ table1
    assert(left.counts.size == 50)
    assert(right.counts.size == 50)
    left.counts.values.foreach(a => {
      assert(a.size == 3)
      a.foreach(b => {
        assert(b.errorsByVariate.size == 3)
        b.errorsByVariate.values.foreach(c => {
          assert(c.basesObserved == 18)
          assert(c.mismatches == 18)
        })
      })
    })
  }

  test("Util :: RecalTable :: Finalization and Deltas :: LargeScale") {
    def _makeTable(qualMax: Int, covarMax: List[Int], obs: Int, mm: Int): RecalTable = {
      assert(covarMax.forall(t => t == covarMax.head))
      val errMap = mutable.HashMap[Int, Array[ErrorCounts]]()
      var expMM = 0.0
      errMap ++= (1 to qualMax).map(qual => {
        val counts = covarMax.map(t => new ErrorCounts).toArray
        (1 to mm).foreach(t => {
          val rg = qual / (1 + RecalUtil.Constants.MAX_REASONABLE_QSCORE)
          val quality = qual - RecalUtil.Constants.MAX_REASONABLE_QSCORE * rg
          val bc = new BaseCovariates(qual, null, quality.toByte, true, false)
          for (covarIdx <- Range(0, covarMax.size)) {
            for (covar <- Range(0, 1 + covarMax(covarIdx))) {
              counts(covarIdx)(covar) += bc
            }
          }
          expMM += RecalUtil.qualToErrorProb(bc.qual) * (1 + covarMax.head)
        })
        (mm + 1 to obs).foreach(t => {
          val rg = qual / (1 + RecalUtil.Constants.MAX_REASONABLE_QSCORE)
          val quality = qual - RecalUtil.Constants.MAX_REASONABLE_QSCORE * rg
          val bc = new BaseCovariates(qual, null, quality.toByte, false, false)
          for (covarIdx <- Range(0, covarMax.size)) {
            for (covar <- Range(0, 1 + covarMax(covarIdx))) {
              counts(covarIdx)(covar) += bc
            }
          }
          expMM += RecalUtil.qualToErrorProb(bc.qual) * (1 + covarMax.head)
        })

        (qual, counts)
      })
      new RecalTable(errMap, expMM)
    }

    // all leaves of the table have 30,000 observations, 30 mismatches (Q30)
    // 10K observations of Q5 (for instance) implies 3162.3 mismatches
    val table = (1 to 10).map(t => _makeTable(120, List(2, 2, 2), 1000, 1)).reduce(_ ++ _)
    table.finalizeTable()
    for (qual <- 1 to 120) {
      for (covar <- cartesian(Array(2, 2, 2))) {
        val baseC = new BaseCovariates(qual, covar, (qual % 60).toByte, false, false) // isMM doesn't matter - this is a lookup
        val expectedCounts = (1 to 60).map(t => 10000 * 3 * 2).sum
        val expectedMismatches = (1 to 60).map(t => 30000 * 2 * RecalUtil.qualToErrorProb(t.toByte)).sum
        val expectedMismatchRate = expectedMismatches / expectedCounts
        val expectedReadGroupShift = 10.0 / 10000 - expectedMismatchRate
        assert(table.globalCounts.basesObserved == expectedCounts)
        assert(table.readGroupCounts(0).basesObserved == 30000 * 60)
        assert(table.readGroupCounts(1).mismatches == 30 * 60)
        assert(table.qualByRGCounts(22).basesObserved == 30000)
        assert(math.abs(expectedReadGroupShift - table.getReadGroupDelta(baseC)) < 1e-12)
        //val expectedShift = RecalUtil.qualToErrorProb(30) - (10000*RecalUtil.qualToErrorProb(baseC.qual).toInt)/10000.0
        //assert(table.getQualScoreDelta())
      }
    }
  }

  sparkTest("Covariate :: QualByRg :: Example") {
    val registrator = System.getProperty("spark.kryo.registrator", "noneFound")
    System.out.println(registrator)
    val rg1 = 0
    val rg2 = 1
    val rg3 = 2
    val qual1 = List(2, 2, 2, 2, 2, 2, 25, 32, 27, 22, 33, 35, 37, 33, 37, 38, 32, 26, 28, 24, 23, 22, 37, 38, 33, 33, 33, 33, 33, 33)
    val qual2 = List(25, 25, 25, 25, 25, 26, 26, 26, 26, 25, 26, 26, 26, 27, 27, 27, 27, 27, 27, 27, 29, 29, 2, 2, 2, 2, 2, 2, 2, 2)
    val qual3 = List(32, 32, 32, 33, 33, 33, 33, 35, 35, 32, 33, 28, 29, 29, 29, 29, 29, 29, 29, 29, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)
    def qualStr(quals: List[Int]): String = quals.map(g => (g + 33).toByte.toChar.toString).reduce(_ + _)
    val rec1 = ADAMRecord.newBuilder().setRecordGroupId(rg1).setQual(qualStr(qual1)).build()
    val rec2 = ADAMRecord.newBuilder().setRecordGroupId(rg2).setQual(qualStr(qual2)).build()
    val rec3 = ADAMRecord.newBuilder().setRecordGroupId(rg3).setQual(qualStr(qual3)).build()
    val records = List(rec1, rec2, rec3)

    val qualByRG = new QualByRG()
    val intervals = List((0, 29), (6, 29), (0, 21), (0, 20))
    for (interval <- intervals) {
      assert(qualByRG(rec1, interval._1, interval._2).deep == qual1.slice(interval._1, interval._2).toArray.deep)
      assert(qualByRG(rec2, interval._1, interval._2).deep == qual2.slice(interval._1, interval._2).map(t => 60 + t).toArray.deep)
      assert(qualByRG(rec3, interval._1, interval._2).deep == qual3.slice(interval._1, interval._2).map(t => 120 + t).toArray.deep)
    }
  }

  /*
  sparkTest("Covariate :: QualByRG :: Combinatorial") {
    def qualStr(quals: List[Int]): CharSequence = quals.map(g => (g + 33).toByte.toChar.toString).reduce(_ + _).asInstanceOf[CharSequence]
    def genQual(length: Int = 101): List[Int] = (1 to length).map(t => 1 + RANDOM.nextInt(59)).toList
    def qualRG2ADAM(rg: CharSequence, quals: List[Int]): ADAMRecord = {
      ADAMRecord.newBuilder().setRecordGroupId(rg).setQual(qualStr(quals)).build()
    }

    for (numRG <- 1 to 50) {
      val readGroupIds = (1 to numRG).map(t => RANDOM.nextString(50).asInstanceOf[CharSequence]).toArray
      for (numReads <- (1 to 50).map(t => t * 10)) {
        // get random qualities
        val qualities = (1 to numRG).map(t => (t - 1, (1 to numReads).map(r => genQual()).toList)).toMap
        val covExpected = qualities.map(kv => {
          val rg = kv._1
          val qualsList = kv._2
          readGroupIds(rg) -> qualsList.map(quals => quals.map(q => 60 * rg + q))
        })
        // map qualities and the read group index to a partial ADAMRecord
        val records = qualities.flatMap(qr => {
          val rg = readGroupIds(qr._1)
          val qualsList = qr._2
          qualsList.map(quals => qualRG2ADAM(rg, quals))
        }).toList
        val recsWithQuals = RANDOM.shuffle(qualities.values.flatMap(identity).zip(records))
        val recordsRDD = sc.makeRDD(recsWithQuals.map(_._2).toList, 1)
        val rgMap = records.map(_.getRecordGroupId).distinct.zipWithIndex.toMap
        val qualByRG = new QualByRG(recordsRDD)
        val covar = recordsRDD.map(r => qualByRG(r, 0, 100)).collect()
      }
    }
  }

  sparkTest("Covariate :: Cycle :: Example") {
    val rg1 = "readGroup1".asInstanceOf[CharSequence]
    val rg2 = "readGroup2".asInstanceOf[CharSequence]
    val rg3 = "readGroup3".asInstanceOf[CharSequence]
    val qual1 = List(2, 2, 2, 2, 2, 2, 25, 32, 27, 22, 33, 35, 37, 33, 37, 38, 32, 26, 28, 24, 23, 22, 37, 38, 33, 33, 33, 33, 33, 33)
    val qual2 = List(25, 25, 25, 25, 25, 26, 26, 26, 26, 25, 26, 26, 26, 27, 27, 27, 27, 27, 27, 27, 29, 29, 2, 2, 2, 2, 2, 2, 2, 2)
    val qual3 = List(32, 32, 32, 33, 33, 33, 33, 35, 35, 32, 33, 28, 29, 29, 29, 29, 29, 29, 29, 29, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)
    val seq1 = qual1.map(t => "A").reduce(_ + _).asInstanceOf[CharSequence]
    val seq2 = qual2.map(t => "C").reduce(_ + _).asInstanceOf[CharSequence]
    val seq3 = qual3.map(t => "G").reduce(_ + _).asInstanceOf[CharSequence]
    def qualStr(quals: List[Int]): CharSequence = quals.map(g => (g + 33).toByte.toChar.toString).reduce(_ + _).asInstanceOf[CharSequence]
    val rec1_fwd = ADAMRecord.newBuilder().setRecordGroupId(rg1).setQual(qualStr(qual1)).
      setSequence(seq1).setReadNegativeStrand(false).setFirstOfPair(true).build()

    val rec2_fwd = ADAMRecord.newBuilder().setRecordGroupId(rg2).setQual(qualStr(qual2)).
      setSequence(seq2).setReadNegativeStrand(true).setFirstOfPair(true).build()

    val rec3_fwd = ADAMRecord.newBuilder().setRecordGroupId(rg3).setQual(qualStr(qual3)).
      setSequence(seq3).setReadNegativeStrand(false).setFirstOfPair(true).build()

    val rec1_rev = ADAMRecord.newBuilder().setRecordGroupId(rg1).setQual(qualStr(qual1)).
      setSequence(seq1).setFirstOfPair(false).setReadNegativeStrand(true).build()

    val rec2_rev = ADAMRecord.newBuilder().setRecordGroupId(rg2).setQual(qualStr(qual2)).
      setSequence(seq2).setFirstOfPair(false).setReadNegativeStrand(false).build()

    val rec3_rev = ADAMRecord.newBuilder().setRecordGroupId(rg3).setQual(qualStr(qual3)).
      setFirstOfPair(false).setSequence(seq3).setReadNegativeStrand(true).build()

    val records = List(rec1_fwd, rec2_fwd, rec3_fwd, rec1_rev, rec2_rev, rec3_rev)
    val recRdd = sc.makeRDD(records, 1)
    val cycleCovar = new DiscreteCycle(recRdd)
    val length = 30
    assert(cycleCovar(rec1_fwd, 0, 29).deep == (1 to 30).toArray.deep)
    assert(cycleCovar(rec1_rev, 0, 29).deep == (1 to 30).map(-_).toArray.deep)
    assert(cycleCovar(rec2_fwd, 10, 20).deep == (11 to 20).toArray.deep)
    assert(cycleCovar(rec3_rev, 10, 20).deep == (11 to 20).map(-_).toArray.deep)
  }*/

  test("Covariate :: Cycle :: Combinatorial") {
    // todo
  }

  test("Covariate :: Context :: Example") {
    val seq1 = "AACCTTGGAA"
    val seq2 = "GGCTACGT"
    val seq3 = "T" * 50 + seq2 + "A" * 55

    // expected contexts
    val seq1_fwd_2 = List(None, "AA", "AC", "CC", "CT", "TT", "TG", "GG", "GA", "AA")
    val seq1_fwd_3 = List(None, None, "AAC", "ACC", "CCT", "CTT", "TTG", "TGG", "GGA", "GAA")
    val seq1_rev_4 = List(None, None, None, "TTCC", "TCCA", "CCAA", "CAAG", "AAGG", "AGGT", "GGTT")
    val seq2_rev_2 = List(None, "AC", "CG", "GT", "TA", "AG", "GC", "CC")
    val seq3_window_fwd_2 = List(None, "GG", "GC", "CT", "TA", "AC", "CG", "GT")
    val seq3_window_rev_3 = List(None, None, "ACG", "CGT", "GTA", "TAG", "AGC", "GCC")
  }

  test("Covariate :: Context :: Combinatorial") {
    // todo -- note that can't inspect sliding window (code is the same as in the class) -- so test invariants
  }

  test("ErrorPosition :: Example") {
    // 'A' mismatched at 54, base at index 86 is insertion
    val cigar1 = "85M1I15M"
    val md1 = "53A46"
    // 'T' deletion between 33 and 34, 'T' mismatched at 36
    val cigar2 = "33M1D23M45S"
    val md2 = "33^T5T17"
    // base at index 18 is insertion, 'T' mismatched at 74, 'C' mismatched at 80 (end of read)
    val cigar3 = "17M1I63M20S"
    val md3 = "73T5C0"
    // C0NJNACXX120630:3:2116:20611:54061
    // 'A' mismatched at 15, base at index 47 is inserted, 'T' mismatched at 81
    val cigar4 = "46M1I38M16S"
    val md4 = "14A65T3"
    // bases at 66,67,68 are insertion
    val cigar5 = "65M3I33M"
    val md5 = "98"
    // 'T' mismatched at 43, 'TAT' deletion between 48 and 49
    val cigar6 = "48M3D53M"
    val md6 = "42T5^TAT53"
    // 'A' mismatched at 33, 'G' mismatched at 56, 'G' deletion between 69 and 70, 'C' mismatched at 71
    // 'T' mismatched at 73, 'A' mismatched at 93  -- md field not updated for soft clips
    val cigar7 = "59M1D26M16S"
    val md7 = "32A12G13^G1C1T19A2"
    // 'A' deletion between 30 and 31, 'T' mismatched at 52, 'A' mismatched at 59, 'T' mismatched at 76 (end of read)
    val cigar8 = "30M1D46M25S"
    val md8 = "30^A21T6A16T0"
    // 'T' mismatched at 37, 'C' mismatched at 59, 'C' mismatched at 65, 'T' mismatched at 91, 'C' mismatched at 100 (end of read)
    val cigar9 = "100M1S"
    val md9 = "36T21C5C25T8C0"
    // everything matches
    val cigar10 = "101M"
    val md10 = "101"
    // everything matches with a softclip
    val cigar11 = "89M12S"
    val md11 = "89"
  }
}
