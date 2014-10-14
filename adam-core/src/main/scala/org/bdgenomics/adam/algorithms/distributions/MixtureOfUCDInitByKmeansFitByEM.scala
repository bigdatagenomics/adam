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
package org.bdgenomics.adam.algorithms.distributions

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.annotation.tailrec
import scala.math.{ abs, log => mathLog, max, pow, sqrt }
import scala.reflect.ClassTag

/**
 * A trait for mixtures of univariate continuous distributions whose initial
 * values are chosen by running k-means, and whose distributions are fit by
 * an EM algorithm.
 */
trait MixtureOfUCDInitByKMeansFitByEM[D <: UnivariateContinuousDistribution]
    extends MixtureOfUnivariateContinuousDistributions[D] with Logging {

  val quitEarly = false

  /**
   * Computes the assignment weights of a single point to the different
   * distributions that we are fitting.
   *
   * @param value The value of this point.
   * @param weighting An array containing the weights of all current distributions.
   * @param distributions An array containing all distributions we have fit.
   * @return Returns a tuple containing the per-point weights of all distributions,
   *         and the expected complete log likelihood contribution of this point.
   */
  protected def classMembership(value: Double,
                                weighting: Array[Double],
                                distributions: Array[D]): (Array[Double], Double) = {
    // find probability densities, then weight
    val densities = distributions.map(_.probabilityDensity(value))
    val weighted = densities.zip(weighting).map(p => p._1 * p._2)

    // perform soft assignment
    val weight = weighted.sum
    val membership = weighted.map(_ / weight)

    // calculate expected complete log likelihood contribution of this point
    val pointEcll = weighted.map(safeLog(_))
      .zip(membership)
      .map(p => p._1 * p._2)
      .reduce(_ + _)

    // return soft assignment and ECLL contribution
    (membership, pointEcll)
  }

  /**
   * Runs an EM loop to fit a mixture model.
   *
   * @param rdd An RDD of doubles to fit the mixture model to.
   * @param initialDistributions The intial distributions to start running EM from.
   * @param maxIterations The maximum number of iterations to run.
   * @param ecllThreshold We stop running the EM loop once the expected complete log
   *                      likelihood increases by less than this threshold.
   * @return Returns an array of fit distributions.
   */
  protected def em(rdd: RDD[Double],
                   initialDistributions: Array[D],
                   maxIterations: Int,
                   initialWeights: Array[Double],
                   ecllThreshold: Option[Double] = None)(
                     implicit dTag: ClassTag[D]): Array[D] = {

    // tail recursive helper function for running em loop
    @tailrec def emLoopHelper(iteration: Int,
                              lastEcll: Double,
                              lastWeighting: Array[Double],
                              lastDistributions: Array[D]): Array[D] = {

      // run expectation stage - generates class assignments,
      // and updates current expected complete log likelihood
      val (classAssignments, ecll) = eStep(rdd,
        lastDistributions,
        lastWeighting)

      if ((quitEarly && ecll < lastEcll) ||
        (ecllThreshold.isDefined && ecll < lastEcll + ecllThreshold.get)) {
        log.info("Quitting on iteration " + iteration + " due to insufficient improvement.")
        lastDistributions
      } else if (iteration > maxIterations) {
        log.info("Quitting as have exceeded max iteration count.")
        lastDistributions
      } else {
        // run maximization stage - updates distributions
        val (distributions,
          weighting) = mStep(classAssignments.zip(rdd), lastDistributions, iteration)

        // print logging info
        log.info("After iteration " + iteration + ":")
        (0 until distributions.length).foreach(i => {
          log.info("Distribution " + i + " has weight " + weighting(i) +
            " and distribuion: " + distributions(i))
        })

        // recursively call next iteration
        emLoopHelper(iteration + 1,
          ecll,
          weighting,
          distributions)
      }
    }

    // call em loop
    val distributions = emLoopHelper(0,
      Double.NegativeInfinity,
      initialWeights,
      initialDistributions)

    // return our two distributions
    distributions
  }

  /**
   * Implements the basic expectation stage for most EM algorithms. Algorithms
   * that diverge from the traditional E step should override this method.
   *
   * @param rdd An RDD of data points.
   * @param distributions An array containing the distributions fit in the
   *                      last iteration. This array should contain k distributions,
   *                      where k is the number of components in the mixture.
   * @param weighting The weights of the different distributions.
   * @return Returns an RDD of assignments to classes, and the total ECLL.
   */
  protected def eStep(rdd: RDD[Double],
                      distributions: Array[D],
                      weighting: Array[Double]): (RDD[Array[Double]], Double) = {

    // get class membership and expected complete log likelihood contribution per point
    val classAndEcll = rdd.map(classMembership(_,
      weighting,
      distributions))
    classAndEcll.cache()

    // get class
    val classM = classAndEcll.map(_._1)

    // reduce down to get expected compute log likelihood contribution
    val ecll = classAndEcll.map(_._2).reduce(_ + _)

    // unpersist temp class & ecll rdd
    classAndEcll.unpersist()

    // return rdd of class assignments, and ecll
    (classM, ecll)
  }

  // aggregation function for arrays which does not allocate a new array
  protected def aggregateArray(a1: Array[Double], a2: Array[Double]): Array[Double] = {
    (0 until a1.length).foreach(i => a1(i) += a2(i))
    a1
  }

  // log function which never returns -infinity or NaN
  protected def safeLog(v: Double, floor: Double = -100000.0): Double = {
    val l = mathLog(v)
    if (l.isNaN || l.isInfinite) {
      floor
    } else {
      l
    }
  }

  /**
   * Initializes the models that we are going to train by EM by using K-means to
   * divide the data points into subgroups with defined mean and standard deviation.
   *
   * @param rdd A RDD of data points.
   * @param k The number of clusters to cut points into.
   * @param maxIterations The maximum number of iterations to execute.
   * @return Returns an array of initialized distributions.
   */
  private def initializeViaKmeans(rdd: RDD[Double],
                                  k: Int,
                                  maxIterations: Int)(implicit dTag: ClassTag[D]): (Array[D], Array[Double]) = {

    // initialize distributions by running k-means
    val clusters = KMeans.train(rdd.map(p => Vectors.dense(p)), k, maxIterations)

    // get cluster centroids
    val centroids = clusters.clusterCenters
      .flatMap(c => c.toArray)
      .toSeq
      .sortWith(_ < _)
    assert(centroids.length == k)
    val pointsPerCentroid = new Array[Long](k)
    val centroidSigmas = new Array[Double](k)

    // collect standard deviations for clusters
    // 0th distribution
    val centroid0Points = if (k > 1) {
      rdd.filter(v => abs(v - centroids(0)) <= abs(v - centroids(1)))
    } else {
      rdd
    }

    if (k > 1) centroid0Points.cache()

    pointsPerCentroid(0) = centroid0Points.count
    centroidSigmas(0) = sqrt(centroid0Points.map(p => pow(p - centroids(0), 2.0)).reduce(_ + _) / pointsPerCentroid(0))

    if (k > 1) centroid0Points.unpersist()

    if (k > 1) {
      // get standard deviations for distributions 1...k - 2
      (1 until (k - 1)).foreach(i => {
        val centroidIPoints = rdd.filter(v => abs(v - centroids(i - 1)) > abs(v - centroids(i)) &&
          abs(v - centroids(i)) <= abs(v - centroids(i + 1)))
        centroidIPoints.cache()
        pointsPerCentroid(i) = centroidIPoints.count
        centroidSigmas(i) = sqrt(centroidIPoints.map(p => pow(p - centroids(i), 2.0)).reduce(_ + _) / pointsPerCentroid(i))
        centroidIPoints.unpersist()
      })

      // (k - 1)th distribution
      val centroidKPoints = rdd.filter(v => abs(v - centroids(k - 2)) > abs(v - centroids(k - 1)))
      centroidKPoints.cache()
      pointsPerCentroid(k - 1) = centroidKPoints.count
      centroidSigmas(k - 1) = sqrt(centroidKPoints.map(p => pow(p - centroids(k - 1), 2.0)).reduce(_ + _) / pointsPerCentroid(k - 1))
      centroidKPoints.unpersist()
    }

    // create distributions
    val initialDistributions = new Array[D](k)
    (0 until k).foreach(i => initialDistributions(i) = initializeDistribution(centroids(i), centroidSigmas(i)))

    // create weights
    val totalPoints = pointsPerCentroid.sum.toDouble
    val weights = pointsPerCentroid.map(p => p.toDouble / totalPoints)

    // print log info
    log.info("After k-means initialization, have:")
    (0 until k).foreach(i => log.info("Distribution " + i + ": " + initialDistributions(i)))

    (initialDistributions, weights)
  }

  /**
   * The maximization stage for the EM algorithm. Mst be implemented by user.
   *
   * @param rdd An RDD containing an array of weights and the value for the point.
   * @param distributions The distributions fit by the last iteration of the EM algorithm.
   * @param iter The number of the current iteration.
   * @return Returns a tuple containing an array of updated distributions as well as
   *         an array of distribution weights (should sum to one).
   */
  protected def mStep(rdd: RDD[(Array[Double], Double)],
                      distributions: Array[D],
                      iter: Int): (Array[D], Array[Double])

  /**
   * Initializes the distributions, given a mean and a sigma.
   *
   * @param mean Mean for an initial distribution.
   * @param sigma Standard deviation for an initial distribution.
   * @return Returns a distribution.
   */
  protected def initializeDistribution(mean: Double, sigma: Double): D

  def fit(rdd: RDD[Double],
          k: Int,
          maxIterations: Int)(implicit dTag: ClassTag[D]): Array[D] = {
    val (initialDistributions, initialWeights) = initializeViaKmeans(rdd, k, maxIterations)
    em(rdd, initialDistributions, maxIterations, initialWeights)
  }

  def fit(rdd: RDD[Double],
          k: Int,
          maxEmIterations: Int,
          maxKmeansIterations: Int,
          ecllThreshold: Option[Double] = None)(implicit dTag: ClassTag[D]): Array[D] = {
    val (initialDistributions, initialWeights) = initializeViaKmeans(rdd, k, maxKmeansIterations)
    em(rdd, initialDistributions, maxEmIterations, initialWeights, ecllThreshold)
  }
}
