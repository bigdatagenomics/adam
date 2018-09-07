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
import org.apache.spark.sql.functions.{ countDistinct, first, row_number, sum, when }
import org.bdgenomics.adam.models.RecordGroupDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.sql.{ AlignmentRecord => AlignmentRecordSchema, Fragment => FragmentSchema }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment, Strand }
import org.bdgenomics.utils.misc.Logging
import htsjdk.samtools.{ Cigar, CigarElement, CigarOperator, TextCigarCodec }
import scala.collection.JavaConversions._

private[rdd] object MarkDuplicates extends Serializable with Logging {

  /**
   * Marks alignment records as PCR duplicates.
   * This class marks duplicates all read pairs that have the same pair alignment locations,
   * and all unpaired reads that map to the same sites. Only the highest scoring
   * read/read pair is kept, where the score is the sum of all quality scores minus 33 in
   * the read that are greater than 15.
   * @param alignmentRecords GenomicRDD of alignment records
   * @return RDD of alignment records with the "duplicateRead" field marked appropriately
   */
  def apply(alignmentRecords: AlignmentRecordRDD): RDD[AlignmentRecord] = {
    import alignmentRecords.dataset.sparkSession.implicits._
    checkRecordGroups(alignmentRecords.recordGroups)

    val libDf = libraryDf(alignmentRecords.recordGroups, alignmentRecords.dataset.sparkSession)
    val fragmentsDf = groupReadsByFragment(alignmentRecords.dataset)
      .join(libDf, "recordGroupName")

    // DataFrame containing all identified duplicates
    val duplicatesDf = findDuplicates(fragmentsDf)

    // mark the identified duplicates in the original Dataset and convert Spark SQL DataFrame back to RDD
    markDuplicates(alignmentRecords.dataset, duplicatesDf)
      .as[AlignmentRecordSchema]
      .rdd.map(_.toAvro)
  }

  /**
   * Groups alignment records (reads) by fragment while finding the reference positions and
   * scores of each mapped read in each fragment.
   * @param alignmentRecords Dataset of alignment records
   * @return DataFrame with rows representing fragments made by grouping together the alignment
   *         records by record group name and read name.
   */
  private def groupReadsByFragment(alignmentRecords: Dataset[AlignmentRecordSchema]): DataFrame = {
    import alignmentRecords.sqlContext.implicits._

    // Find the 5' position of all alignment records
    val df = alignmentRecords
      .withColumn("fivePrimePosition",
        fivePrimePositionUDF('readMapped, 'readNegativeStrand, 'cigar, 'start, 'end))

    // Group all fragments, finding read 1 & 2 reference positions and scores
    df
      .groupBy("recordGroupName", "readName")
      .agg(

        // Read 1 reference position (contig name)
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped, 'contigName).otherwise('sequence)),
          ignoreNulls = true)
          as 'read1contigName,

        // Read 1 reference position (5' position)
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped, 'fivePrimePosition).otherwise(0L)),
          ignoreNulls = true)
          as 'read1fivePrimePosition,

        // Read 1 reference position (strand)
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped,
            when('readNegativeStrand, Strand.REVERSE.toString).otherwise(Strand.FORWARD.toString))
            .otherwise(Strand.INDEPENDENT.toString)),
          ignoreNulls = true)
          as 'read1strand,

        // Read 2 reference position (contig name)
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped, 'contigName).otherwise('sequence)),
          ignoreNulls = true)
          as 'read2contigName,

        // Read 2 reference position (5' position)
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped, 'fivePrimePosition).otherwise(0L)),
          ignoreNulls = true)
          as 'read2fivePrimePosition,

        // Read 2 reference position (strand)
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped,
            when('readNegativeStrand, Strand.REVERSE.toString).otherwise(Strand.FORWARD.toString))
            .otherwise(Strand.INDEPENDENT.toString)),
          ignoreNulls = true)
          as 'read2strand,

        // Fragment score
        sum(when('readMapped and 'primaryAlignment, scoreReadUDF('qual))) as 'score)
  }

  /**
   * Identifies duplicates among a collection of fragments. Duplicates are those fragments which are not the
   * highest scoring fragment among those with the same left and right reference positions or those with unmapped
   * right position and group count is equal to zero.
   * @param fragmentDf A DataFrame representing genomic fragments with the following schema:
   *                   "library", "recordGroupName", "readName", "score",
   *                   "read1contigName", "read1fivePrimePosition", "read1strand",
   *                   "read2contigName", "read2fivePrimePosition", "read2strand"
   * @return A DataFrame with the following schema "recordGroupName", "readName", "duplicateFragment"
   *         indicating all of the fragments which have duplicate reads in them in the "duplicateFragment"
   *         column, which contains booleans.
   */
  private def findDuplicates(fragmentDf: DataFrame): DataFrame = {
    import fragmentDf.sparkSession.implicits._

    // this DataFrame has an extra column "groupCount" which is the number of distinct
    // right reference positions for fragments grouped by left reference position
    val withGroupCount = calculateGroupCounts(fragmentDf)

    // Window into fragments grouped by left and right reference positions
    val positionWindow = Window.partitionBy(
      'library,
      'read1contigName, 'read1fivePrimePosition, 'read1strand,
      'read2contigName, 'read2fivePrimePosition, 'read2strand)
      .orderBy('score.desc)

    // duplicates are those fragments which are not the highest scoring fragment among those with the same
    // left and right reference positions or those with unmapped right position and group count is equal to zero
    val duplicatesDf = withGroupCount.withColumn("duplicateFragment",
      ('read1contigName.isNotNull and 'read1fivePrimePosition.isNotNull and 'read1strand.isNotNull)
        and (
          row_number.over(positionWindow) =!= 1
          or ('read2contigName.isNull and 'read2fivePrimePosition.isNull and 'read2strand.isNull and 'groupCount > 0)))

    // result is just the relation between fragment and duplicate status
    duplicatesDf.select("recordGroupName", "readName", "duplicateFragment")
  }

  /**
   * Calculates the number of distinct right positions for each group of left reference positions
   * and joins this information back with the original DataFrame
   * @param fragmentDf DataFrame of fragments containing left and right reference positions
   * @return DataFrame identical to `fragmentDf` but with additional column "groupCount" equal to the number of
   *         distinct right reference positions among all fragments with the same left reference position as that row
   */
  private def calculateGroupCounts(fragmentDf: DataFrame): DataFrame = {

    // count number of distinct right-positions for each left-position
    val groupCountDf = countRightGroups(fragmentDf)

    // join the group count info into the original dataframe (saving null reference positions
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
   * Marks each alignment record in the Dataset `alignmentRecords` as a duplicate based on duplicate information
   * from a DataFrame specifying which fragments found in the collection of alignment records are duplicates.
   * Each read will be marked as a duplicate if it is a primary alignments and part of a duplicate fragment
   * or if it is a mapped read but not a primary alignment.
   * Unmapped reads will not be marked as duplicate.
   * @param alignmentRecords Dataset of AlignmentRecords
   * @param duplicatesDf DataFrame containing information about each
   * @return A DataFrame with the same schema as `alignmentRecords`  but with reads having
   *         been marked as duplicates in the "duplicateRead" column in accordance with `duplicatesDf`
   *         DataFrame which was provided.
   */
  private def markDuplicates(alignmentRecords: Dataset[AlignmentRecordSchema], duplicatesDf: DataFrame): DataFrame = {
    import alignmentRecords.sparkSession.implicits._
    addDuplicateFragmentInfo(alignmentRecords, duplicatesDf)
      .withColumn("duplicateRead",
        'duplicateFragment.isNotNull and 'readMapped and ('duplicateFragment or !'primaryAlignment))
      .drop("duplicateFragment") // drop the temporary column for marking duplicate fragments
  }

  /**
   * Adds information about which fragments are duplicates given in `duplicatesDf` to a dataset of alignment records
   * @param alignmentRecords Dataset of alignment records to add duplicate fragment info to
   * @param duplicatesDf DataFrame with columns "recordGroupName", "readName" and "duplicateFragment" indicating
   *                     for each fragment (identified by "record
   * @return A new DataFrame identical to `alignmentRecords` but with an extra boolean column "duplicateFragment"
   *         indicating for each alignment record whether or not it is part of a duplicate fragment
   */
  private def addDuplicateFragmentInfo(alignmentRecords: Dataset[AlignmentRecordSchema],
                                       duplicatesDf: DataFrame): DataFrame = {
    import alignmentRecords.sparkSession.implicits._
    alignmentRecords.join(duplicatesDf, Seq("readName", "recordGroupName"), "left")
  }

  /**
   * Case class which merely extends the Fragment Schema by a single column "duplicateFragment" so that
   * a DataFrame with fragments having been marked as duplicates can be cast back into a DataSet
   */
  private case class FragmentDuplicateSchema(readName: Option[String] = None,
                                             instrument: Option[String] = None,
                                             runId: Option[String] = None,
                                             fragmentSize: Option[Int] = None,
                                             duplicateFragment: Option[Boolean] = None,
                                             alignments: Seq[AlignmentRecordSchema] = Seq())

  /**
   * Marks fragments as duplicate
   *
   * @param fragmentRdd A genomic RDD representing a collection of fragments
   * @return A RDD of fragments each having been specified as duplicate or not
   */
  def apply(fragmentRdd: FragmentRDD): RDD[Fragment] = {
    import fragmentRdd.dataset.sparkSession.implicits._

    // convert fragments to DataFrame with reference positions and scores
    val fragmentDf = fragmentRdd.rdd
      .map(toFragmentSchema(_, fragmentRdd.recordGroups))
      .toDF(
        "library", "recordGroupName", "readName",
        "read1contigName", "read1fivePrimePosition", "read1strand",
        "read2contigName", "read2fivePrimePosition", "read2strand",
        "score")

    // find the duplicates (top scoring fragments after grouping by left and right position)
    val duplicatesDf = findDuplicates(fragmentDf)

    markDuplicateFragments(fragmentRdd.dataset, duplicatesDf)
      .rdd.map(_.toAvro)
  }

  private def markDuplicateFragments(fragmentDs: Dataset[FragmentSchema],
                                     duplicatesDf: DataFrame): Dataset[FragmentSchema] = {

    def isDuplicate(readMapped: Option[Boolean], duplicateFragment: Option[Boolean],
                    primaryAlignment: Option[Boolean]): Boolean = {
      readMapped.getOrElse(false) && (duplicateFragment.getOrElse(false) || !primaryAlignment.getOrElse(false))
    }

    def toMarkedFragment(fragment: FragmentDuplicateSchema): FragmentSchema = {
      FragmentSchema(
        fragment.readName,
        fragment.instrument,
        fragment.runId,
        fragment.fragmentSize,
        fragment.alignments.map(alignment => {
          val alignmentRecord = alignment.toAvro
          val isdup = isDuplicate(alignment.readMapped, fragment.duplicateFragment, alignment.primaryAlignment)
          alignmentRecord.setDuplicateRead(isdup)
          AlignmentRecordSchema.fromAvro(alignmentRecord)
        }))
    }

    import fragmentDs.sparkSession.implicits._
    fragmentDs.join(duplicatesDf, Seq("readName"), "left")
      .as[FragmentDuplicateSchema]
      .map(toMarkedFragment)
  }

  /**
   * Converts a fragment to a tuple suitable for use as a row in a DataFrame with schema `FragmentSchema`
   * @param fragment The fragment to convert into a tuple
   * @param recordGroups Dictionary mapping record group name to library
   * @return Tuple containing library, record group name, read name, score, and left and right reference positions.
   *         For the fragment. The left and right reference positions are given by the reference name, five prime
   *         position and strand of the first and second read in the fragment, respectively.
   */
  private def toFragmentSchema(fragment: Fragment, recordGroups: RecordGroupDictionary) = {
    val bucket = SingleReadBucket(fragment)
    val position = ReferencePositionPair(bucket)

    val recordGroupName: Option[String] = bucket.allReads.headOption.flatMap(r => Some(r.getRecordGroupName))
    val library: Option[String] = recordGroupName.flatMap(name => if (name == null) None else recordGroups(name).library)

    // reference positions of each read in the fragment
    val read1refPos = position.read1refPos
    val read2refPos = position.read2refPos

    // tuple that will be turned into a row in the DataFrame
    (library, recordGroupName, fragment.getReadName,
      read1refPos.map(_.referenceName), read1refPos.map(_.pos), read1refPos.map(_.strand.toString),
      read2refPos.map(_.referenceName), read2refPos.map(_.pos), read2refPos.map(_.strand.toString),
      scoreBucket(bucket))
  }

  /* User defined aggregate function for calculating the "score" of a fragment */
  private def scoreReadUDF = functions.udf((qual: String) => scoreRead(qual))

  /**
   * Scores a single alignment record by summing all quality scores in the read
   * which are greater than 15.
   * @param record Alignment record containing quality scores
   * @return The "score" of the read, given by the sum of all quality scores greater than 15
   */
  def score(record: AlignmentRecord): Int = {
    record.qualityScores.filter(15 <= _).sum
  }

  /**
   * Scores a single read based on it's quality.
   * @param qual Base64 encoded quality score string
   * @return Sum of quality score minus 33 for all quality scores GTE 15
   */
  private def scoreRead(qual: String): Int = {
    qual.toCharArray.map(q => q - 33).filter(15 <= _).sum
  }

  /**
   * Calculates the score for a bucket of reads. The score is given by
   * the sum of quality scores for each primary aligned read in the bucket
   * @param bucket collection of reads
   * @return sum of quality scores for each primary mapped read in the bucket
   */
  private def scoreBucket(bucket: SingleReadBucket): Int = {
    bucket.primaryMapped.map(score).sum
  }

  /* Determines if a Cigar Element operator is a clipp*/
  private def isClipped(el: CigarElement): Boolean = {
    el.getOperator == CigarOperator.SOFT_CLIP || el.getOperator == CigarOperator.HARD_CLIP
  }

  /* Spark SQL UDF wrapper for finding the 5' reference position of an alignment. */
  private def fivePrimePositionUDF = functions.udf(
    (readMapped: Boolean, readNegativeStrand: Boolean, cigar: String, start: Long, end: Long) =>
      fivePrimePosition(readMapped, readNegativeStrand, cigar, start, end))

  /**
   * Determines the five prime reference position for an alignment record by discarding clipped base pairs
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
