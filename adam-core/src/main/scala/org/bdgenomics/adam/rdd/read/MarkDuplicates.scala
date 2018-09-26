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
package org.bdgenomics.adam.rdd.read

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import org.apache.spark.sql.functions.{ countDistinct, first, rank, row_number, sum, when }
import org.bdgenomics.adam.models.RecordGroupDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.{ DatasetBoundFragmentRDD, FragmentRDD, RDDBoundFragmentRDD }
import org.bdgenomics.adam.sql.{ AlignmentRecord => AlignmentRecordProduct, Fragment => FragmentProduct }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment, Strand }
import org.bdgenomics.utils.misc.Logging
import htsjdk.samtools.{ Cigar, CigarElement, CigarOperator, TextCigarCodec }
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructField

import scala.collection.JavaConversions._

private[rdd] object MarkDuplicates extends Serializable with Logging {

  /**
   * Identifies and marks alignment records as duplicates
   * Duplicates are identified as follows:
   * Among all fragments that have the same left and right positions,
   * duplicate fragments are all those which are not the highest quality fragment.
   * Duplicate reads are those reads which are mapped and are in a duplicate fragment
   * or are not primary alignments.
   *
   * @param alignmentRecords collection of alignment records to identify duplicates within
   * @param recordGroupDictionary mapping from record group name to library
   * @return the same collection of alignment records identical to `alignmentRecords` but with
   *         duplicates having been marked in the `duplicateRead` field.
   */
  def apply(alignmentRecords: RDD[AlignmentRecord],
            recordGroupDictionary: RecordGroupDictionary): RDD[AlignmentRecord] = {

    val sqlContext = SQLContext.getOrCreate(alignmentRecords.context)
    import sqlContext.implicits._
    val dataset = sqlContext.createDataset(alignmentRecords.map(AlignmentRecordProduct.fromAvro))

    MarkDuplicates(dataset, recordGroupDictionary)
      .rdd.map(_.toAvro)
  }

  /**
   * Identifies and marks fragments as duplicates
   *
   * @param fragments collection of fragments to mark duplicates within
   * @param recordGroupDictionary mapping from record group name to library
   * @param dummy type erasure work-around
   * @return RDD of fragments identical to input RDD `fragments` but with alignment records (reads)
   *         within the fragments having been marked as duplicates according to the duplciate marking algorithm
   */
  def apply(fragments: RDD[Fragment],
            recordGroupDictionary: RecordGroupDictionary)(implicit dummy: DummyImplicit): RDD[Fragment] = {

    val sqlContext = SQLContext.getOrCreate(fragments.context)
    import sqlContext.implicits._
    val dataset = sqlContext.createDataset(fragments.map(FragmentProduct.fromAvro))

    MarkDuplicates(dataset, recordGroupDictionary)
      .rdd.map(_.toAvro)
  }

  /**
   * Identifies duplicate among a collection of alignment records
   *
   * @param alignmentRecords Dataset of alignment records to mark duplicates in
   * @param recordGroupDictionary A mapping from record group name to library
   * @return Dataset of alignment records with duplicate alignment records having been marked
   *         in the 'duplicateRead' field.
   */
  def apply(alignmentRecords: Dataset[AlignmentRecordProduct],
            recordGroupDictionary: RecordGroupDictionary): Dataset[AlignmentRecordProduct] = {
    import alignmentRecords.sparkSession.implicits._
    checkRecordGroups(recordGroupDictionary)

    val libDf = libraryDf(recordGroupDictionary, alignmentRecords.sparkSession)

    val fragmentsDf = addFragmentInfo(alignmentRecords)
      .join(libDf, Seq("recordGroupName"), "left")

    val withGroupCountDf = calculateGroupCounts(fragmentsDf)

    identifyDuplicateAlignments(withGroupCountDf)
      .as[AlignmentRecordProduct]
  }

  /**
   * Adds fragment related information to the a Dataset of alignment records. This includes
   * the fivePrimePosition of the read, the fragment's score, and the reference positions of the
   * read1 and read2 alignment records in the fragment.
   *
   * @param alignmentRecords Dataset of alignment records to add fragment information to
   * @return DataFrame identical but with the additional columns:
   *         "fivePrimePosition", "score",
   *         "read1contigName", "read1fivePrimePosition", "read1strand",
   *         "read2contigName", "read2fivePrimePosition", "read2strand"
   */
  private def addFragmentInfo(alignmentRecords: Dataset[AlignmentRecordProduct]): DataFrame = {
    import alignmentRecords.sqlContext.implicits._

    val fragmentWindow = Window.partitionBy('recordGroupName, 'readName)

    alignmentRecords
      .withColumn("fivePrimePosition",
        fivePrimePositionUDF('readMapped, 'readNegativeStrand, 'cigar, 'start, 'end))

      // Fragment score
      .withColumn("score",
        sum(when('readMapped and 'primaryAlignment, scoreReadUDF('qual)))
          over fragmentWindow)

      // Read 1 reference position (contig name)
      .withColumn("read1contigName",
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped, 'contigName).otherwise('sequence)),
          ignoreNulls = true) over fragmentWindow)

      // Read 1 reference position (5' position)
      .withColumn("read1fivePrimePosition",
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped, 'fivePrimePosition).otherwise(0L)),
          ignoreNulls = true) over fragmentWindow)

      // Read 1 reference position (strand)
      .withColumn("read1strand",
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped,
            when('readNegativeStrand, Strand.REVERSE.toString).otherwise(Strand.FORWARD.toString))
            .otherwise(Strand.INDEPENDENT.toString)),
          ignoreNulls = true) over fragmentWindow)

      // Read 2 reference position (contig name)
      .withColumn("read2contigName",
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped, 'contigName).otherwise('sequence)),
          ignoreNulls = true) over fragmentWindow)

      // Read 2 reference position (5' position)
      .withColumn("read2fivePrimePosition",
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped, 'fivePrimePosition).otherwise(0L)),
          ignoreNulls = true) over fragmentWindow)

      // Read 2 reference position (strand)
      .withColumn("read2strand",
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped,
            when('readNegativeStrand, Strand.REVERSE.toString).otherwise(Strand.FORWARD.toString))
            .otherwise(Strand.INDEPENDENT.toString)),
          ignoreNulls = true) over fragmentWindow)
  }

  /**
   * Identifies duplicate alignment records
   *
   * @param alignmentDf DataFrame of alignment records
   * @return DataFrame of alignment records identical to that passed in but with
   *         duplicated marked in the "duplicateRead" column
   */
  private def identifyDuplicateAlignments(alignmentDf: DataFrame): DataFrame = {
    import alignmentDf.sparkSession.implicits._

    val positionWindow = Window.partitionBy(
      'library,
      'read1contigName, 'read1fivePrimePosition, 'read1strand,
      'read2contigName, 'read2fivePrimePosition, 'read2strand)
      .orderBy('score.desc)

    alignmentDf
      .withColumn("topScoringFragment",
        functions.rank.over(positionWindow) === 1
          and 'readName === first('readName).over(positionWindow)
          and 'recordGroupName === first('recordGroupName).over(positionWindow))
      .withColumn("duplicateFragment",
        !'topScoringFragment
          or ('read2contigName.isNull and 'read2fivePrimePosition.isNull and 'read2strand.isNull and 'groupCount > 0))
      .withColumn("duplicateRead",
        'read1contigName.isNotNull and 'read1fivePrimePosition.isNotNull and 'read1strand.isNotNull
          and
          'readMapped and (!'primaryAlignment or 'duplicateFragment))
      .drop("topScoringFragment")
      .drop("duplicateFragment")
  }

  /**
   * Calculates the number of distinct right positions for each group of left reference positions
   * and joins this information back with the original DataFrame.
   *
   * @param fragmentDf DataFrame of fragments containing left and right reference positions
   * @return DataFrame identical to `fragmentDf` but with additional column "groupCount" equal to the number of
   *         distinct right reference positions among all fragments with the same left reference position as that row
   */
  private def calculateGroupCounts(fragmentDf: DataFrame): DataFrame = {

    // count number of distinct right-positions for each left-position
    val groupCountDf = countRightGroups(fragmentDf)

    // join the group count info into the original DataFrame (saving null reference positions
    fragmentDf.join(groupCountDf,
      (fragmentDf("library") === groupCountDf("library").alias("library_alias") or
        (fragmentDf("library").isNull and groupCountDf("library").isNull)) and
        fragmentDf("read1contigName") === groupCountDf("read1contigName").alias("contigName_alias") and
        fragmentDf("read1fivePrimePosition") === groupCountDf("read1fivePrimePosition").alias("5P_alias") and
        fragmentDf("read1strand") === groupCountDf("read1strand").alias("strand_alias"),
      "left")
      .drop(groupCountDf("library"))
      .drop(groupCountDf("read1contigName"))
      .drop(groupCountDf("read1fivePrimePosition"))
      .drop(groupCountDf("read1strand"))
  }

  /**
   * Calculates the number of distinct right positions for each group of left reference positions
   *
   * @param fragmentsDf DataFrame of fragments containing left and right reference positions
   * @return DataFrame indicating for each left-reference position the group "groupCount" which is equal to the
   *         number of distinct right reference positions among all fragments with the same left
   *         reference position as that row
   */
  private def countRightGroups(fragmentsDf: DataFrame): DataFrame = {
    import fragmentsDf.sparkSession.implicits._
    fragmentsDf.groupBy('library, 'read1contigName, 'read1fivePrimePosition, 'read1strand) // left position
      .agg(countDistinct('read2contigName, 'read2fivePrimePosition, 'read2strand) // right position
        as 'groupCount)
  }

  /**
   * Identifies duplicate reads within fragments contained in `fragments` and updates their duplicate
   * field accordingly.
   *
   * @param fragments Dataset of fragments containing potentially duplicate reads
   * @param recordGroups Mapping from record group name to library
   * @param dummy type erasure work-around
   * @return Dataset of fragments containing alignment records (reads) having been marked
   *         as duplicate according to the duplicate marking algorithm.
   */
  def apply(fragments: Dataset[FragmentProduct],
            recordGroups: RecordGroupDictionary)(implicit dummy: DummyImplicit): Dataset[FragmentProduct] = {
    import fragments.sparkSession.implicits._

    val fragmentDf = addFragmentInfo(fragments, recordGroups)
    val withGroupCountDf = calculateGroupCounts(fragmentDf)
    val markedFragmentsDs = identifyDuplicateFragments(withGroupCountDf)

    // update the field within the reads to reflect the identified duplicate reads
    updateDuplicateAlignments(markedFragmentsDs)
  }

  /**
   * Adds the following fields into the fragment schema to use in the duplicate marking algorithm:
   * "library", "recordGroupName",
   * "read1contigName", "read1fivePrimePosition", "read1strand",
   * "read2contigName", "read2fivePrimePosition", "read2strand",
   * "score"
   *
   * @param fragmentDs Dataset of Fragments
   * @param recordGroups mapping from record group name to library
   * @return DataFrame containing all the same fragments in the input Dataset but
   *         with the extra fields specified above
   */
  private def addFragmentInfo(fragmentDs: Dataset[FragmentProduct], recordGroups: RecordGroupDictionary): DataFrame = {
    import fragmentDs.sparkSession.implicits._

    def toFragmentProduct(fragment: FragmentProduct, recordGroups: RecordGroupDictionary) = {
      val bucket = SingleReadBucket(fragment.toAvro)
      val position = ReferencePositionPair(bucket)

      val recordGroupName: Option[String] = bucket.allReads.headOption.flatMap(
        _.getRecordGroupName match {
          case null         => None
          case name: String => Some(name)
        })
      val library: Option[String] = recordGroupName.flatMap(name => if (name == null) None else recordGroups(name).library)

      // reference positions of each read in the fragment
      val read1refPos = position.read1refPos
      val read2refPos = position.read2refPos

      // tuple that will be turned into a row in the DataFrame
      (fragment.readName, fragment.instrument, fragment.runId, fragment.fragmentSize, fragment.alignments,
        library, recordGroupName,
        read1refPos.map(_.referenceName), read1refPos.map(_.pos), read1refPos.map(_.strand.toString),
        read2refPos.map(_.referenceName), read2refPos.map(_.pos), read2refPos.map(_.strand.toString),
        scoreBucket(bucket))
    }

    fragmentDs.map(toFragmentProduct(_, recordGroups))
      .toDF("readName", "instrument", "runId", "fragmentSize", "alignments",
        "library", "recordGroupName",
        "read1contigName", "read1fivePrimePosition", "read1strand",
        "read2contigName", "read2fivePrimePosition", "read2strand",
        "score")
  }

  /**
   * Case class which merely extends the Fragment Schema by a single column "duplicateFragment" so that
   * a DataFrame with fragments having been marked as duplicates can be cast back into a Dataset
   */
  private case class FragmentDuplicateProduct(readName: Option[String] = None,
                                              instrument: Option[String] = None,
                                              runId: Option[String] = None,
                                              fragmentSize: Option[Int] = None,
                                              duplicateFragment: Option[Boolean] = None,
                                              alignments: Seq[AlignmentRecordProduct] = Seq())

  /**
   * Identifies duplicate fragments.
   *
   * @param fragmentDf DataFrame of fragments to identify as duplciate. This should contain a score and library
   *                   for each fragment as well as the reference positions of read 1 and 2.
   * @return Dataset of Fragments having been marked as duplicate in the "duplicateFragment" field of the schema
   */
  private def identifyDuplicateFragments(fragmentDf: DataFrame): Dataset[FragmentDuplicateProduct] = {
    import fragmentDf.sparkSession.implicits._

    val positionWindow = Window.partitionBy(
      'library,
      'read1contigName, 'read1fivePrimePosition, 'read1strand,
      'read2contigName, 'read2fivePrimePosition, 'read2strand)
      .orderBy('score.desc)

    fragmentDf
      .withColumn("topScoringFragment",
        functions.rank.over(positionWindow) === 1
          and 'readName === first('readName).over(positionWindow)
          and 'recordGroupName === first('recordGroupName).over(positionWindow))
      .withColumn("duplicateFragment",
        !'topScoringFragment
          or ('read2contigName.isNull and 'read2fivePrimePosition.isNull and 'read2strand.isNull and 'groupCount > 0))
      .as[FragmentDuplicateProduct]
  }

  /**
   * Updates the "duplicateRead" field of the alignment records (read) contained within each
   * fragment of `fragments` to reflect the duplicate fragments
   *
   * @param fragments Dataset of fragments to mark as duplicates
   * @return Dataset identical to input Dataset `fragments` but with alignment records within each
   *         fragment having been marked as duplicates according to the duplicateFragment field of each fragment
   */
  private def updateDuplicateAlignments(fragments: Dataset[FragmentDuplicateProduct]): Dataset[FragmentProduct] = {
    import fragments.sparkSession.implicits._

    def isDuplicate(readMapped: Option[Boolean], duplicateFragment: Option[Boolean],
                    primaryAlignment: Option[Boolean]): Boolean = {
      readMapped.getOrElse(false) && (duplicateFragment.getOrElse(false) || !primaryAlignment.getOrElse(false))
    }

    fragments.map(fragment => FragmentProduct(
      fragment.readName, fragment.instrument, fragment.runId, fragment.fragmentSize,
      fragment.alignments.map(alignment => {
        val alignmentRecord = alignment.toAvro
        val isdup = isDuplicate(alignment.readMapped, fragment.duplicateFragment, alignment.primaryAlignment)
        alignmentRecord.setDuplicateRead(isdup)
        AlignmentRecordProduct.fromAvro(alignmentRecord)
      })))
  }

  /* User defined aggregate function for calculating the "score" of a fragment */
  private def scoreReadUDF = functions.udf((qual: String) => scoreRead(qual))

  /**
   * Scores a single alignment record by summing all quality scores in the read
   * which are greater than 15.
   *
   * @param record Alignment record containing quality scores
   * @return The "score" of the read, given by the sum of all quality scores greater than 15
   */
  def score(record: AlignmentRecord): Int = {
    record.qualityScores.filter(15 <= _).sum
  }

  /**
   * Scores a single read based on it's quality.
   *
   * @param qual Base64 encoded quality score string
   * @return Sum of quality score minus 33 for all quality scores GTE 15
   */
  private def scoreRead(qual: String): Int = {
    qual.toCharArray.map(q => q - 33).filter(15 <= _).sum
  }

  /**
   * Calculates the score for a bucket of reads. The score is given by
   * the sum of quality scores for each primary aligned read in the bucket
   *
   * @param bucket collection of reads
   * @return sum of quality scores for each primary mapped read in the bucket
   */
  private def scoreBucket(bucket: SingleReadBucket): Int = {
    bucket.primaryMapped.map(score).sum
  }

  /* Determines if a Cigar Element operator is a clipping operator */
  private def isClipped(el: CigarElement): Boolean = {
    el.getOperator == CigarOperator.SOFT_CLIP || el.getOperator == CigarOperator.HARD_CLIP
  }

  /* Spark SQL UDF wrapper for finding the 5' reference position of an alignment. */
  private def fivePrimePositionUDF = functions.udf(
    (readMapped: Boolean, readNegativeStrand: Boolean, cigar: String, start: Long, end: Long) =>
      fivePrimePosition(readMapped, readNegativeStrand, cigar, start, end))

  /**
   * Determines the five prime reference position for an alignment record by discarding clipped base pairs
   *
   * @param readMapped Whether the read is mapped to a reference
   * @param readNegativeStrand Whether the mapping is on the negative strand
   * @param cigar Cigar string describing the alignment
   * @param start reference position of the start of the alignment
   * @param end reference position of the end of the alignment
   * @return Reference position of the start of the alignment. For mapped reads this means discarding
   *         clipped base pairs form the start and end of the alignment depending on whether the alignment
   *         is on the positive or negative strand, respectively.
   */
  private def fivePrimePosition(readMapped: Boolean, readNegativeStrand: Boolean, cigar: String,
                                start: Long, end: Long): Long = {
    if (!readMapped) 0L
    else {
      val samtoolsCigar = TextCigarCodec.decode(cigar)
      val cigarElements = samtoolsCigar.getCigarElements
      math.max(0L,
        if (readNegativeStrand) {
          cigarElements.reverse.takeWhile(isClipped).foldLeft(end)({
            (pos, cigarEl) => pos + cigarEl.getLength
          })
        } else {
          cigarElements.takeWhile(isClipped).foldLeft(start)({
            (pos, cigarEl) => pos - cigarEl.getLength
          })
        })
    }
  }

  /**
   * Checks the record group dictionary that will be used to group reads by position, issuing a
   * warning if there are record groups where the library name is not set. In this case
   * as all record groups without a library will be treated as coming from a single library.
   *
   * @param recordGroupDictionary A mapping from record group name to library
   */
  private def checkRecordGroups(recordGroupDictionary: RecordGroupDictionary): Unit = {
    val emptyRgs = recordGroupDictionary.recordGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(recordGroup => {
      log.warn(s"Library ID is empty for record group ${recordGroup.recordGroupName} from sample ${recordGroup.sample}.")
    })

    if (emptyRgs.nonEmpty) {
      log.warn("For duplicate marking, all reads whose library is unknown will be treated as coming from the same library.")
    }
  }

  /**
   * Creates a DataFrame with two columns: "recordGroupName" and "library"
   * which maps record group names to library
   *
   * @param recordGroupDictionary A mapping from record group name to library
   * @return A DataFrame with columns "recordGroupName" and "library" representing the
   *         same mapping from record group name to library that was found in the record
   *         group dictionary
   */
  private def libraryDf(recordGroupDictionary: RecordGroupDictionary, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    recordGroupDictionary.recordGroupMap.mapValues(value => {
      val (recordGroup, _) = value
      recordGroup.library
    }).toSeq.toDF("recordGroupName", "library")
  }
}
