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
package org.bdgenomics.adam.apis.java

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.features.FeatureRDD
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variation.{
  DatabaseVariantAnnotationRDD,
  GenotypeRDD,
  VariantRDD
}
import org.bdgenomics.formats.avro._
import scala.collection.JavaConversions._

object JavaADAMContext {
  // convert to and from java/scala implementations
  implicit def fromADAMContext(ac: ADAMContext): JavaADAMContext = new JavaADAMContext(ac)
  implicit def toADAMContext(jac: JavaADAMContext): ADAMContext = jac.ac
}

class JavaADAMContext private (val ac: ADAMContext) extends Serializable {

  /**
   * @return Returns the Java Spark Context associated with this Java ADAM Context.
   */
  def getSparkContext: JavaSparkContext = new JavaSparkContext(ac.sc)

  /**
   * Loads in an ADAM read file. This method can load SAM, BAM, and ADAM files.
   *
   * @param filePath Path to load the file from.
   * @return Returns a read RDD.
   */
  def loadAlignments(filePath: java.lang.String): AlignmentRecordRDD = {
    ac.loadAlignments(filePath)
  }

  /**
   * Loads in sequence fragments.
   *
   * Can load from FASTA or from Parquet encoded NucleotideContigFragments.
   *
   * @param filePath Path to load the file from.
   * @return Returns a NucleotideContigFragment RDD.
   */
  def loadSequences(filePath: java.lang.String): NucleotideContigFragmentRDD = {
    ac.loadSequences(filePath)
  }

  /**
   * Loads in read pairs as fragments.
   *
   * @param filePath The path to load the file from.
   * @return Returns a FragmentRDD.
   */
  def loadFragments(filePath: java.lang.String): FragmentRDD = {
    ac.loadFragments(filePath)
  }

  /**
   * Loads in features.
   *
   * @param filePath The path to load the file from.
   * @return Returns a FeatureRDD.
   */
  def loadFeatures(filePath: java.lang.String): FeatureRDD = {
    ac.loadFeatures(filePath)
  }

  /**
   * Loads in variant annotations.
   *
   * @param filePath The path to load the file from.
   * @return Returns a DatabaseVariantAnnotationRDD.
   */
  def loadVariantAnnotations(filePath: java.lang.String): DatabaseVariantAnnotationRDD = {
    ac.loadVariantAnnotations(filePath)
  }

  /**
   * Loads in genotypes.
   *
   * @param filePath The path to load the file from.
   * @return Returns a GenotypeRDD.
   */
  def loadGenotypes(filePath: java.lang.String): GenotypeRDD = {
    ac.loadGenotypes(filePath)
  }

  /**
   * Loads in variants.
   *
   * @param filePath The path to load the file from.
   * @return Returns a VariantRDD.
   */
  def loadVariants(filePath: java.lang.String): VariantRDD = {
    ac.loadVariants(filePath)
  }
}
