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

import htsjdk.samtools.{ TextCigarCodec, ValidationStringency }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ MdTag, ReferenceRegion }
import org.bdgenomics.adam.util.ReferenceFile
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.Logging

private[read] case class MDTagging(
    reads: RDD[AlignmentRecord],
    @transient referenceFile: ReferenceFile,
    overwriteExistingTags: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.STRICT) extends Logging {
  @transient val sc = reads.sparkContext

  val mdTagsAdded = sc.accumulator(0L, "MDTags Added")
  val mdTagsExtant = sc.accumulator(0L, "MDTags Extant")
  val numUnmappedReads = sc.accumulator(0L, "Unmapped Reads")
  val incorrectMDTags = sc.accumulator(0L, "Incorrect Extant MDTags")

  val taggedReads = addMDTagsBroadcast.cache

  def maybeMDTagRead(read: AlignmentRecord, refSeq: String): AlignmentRecord = {

    val cigar = TextCigarCodec.decode(read.getCigar)
    val mdTag = MdTag(read.getSequence, refSeq, cigar, read.getStart)
    if (read.getMismatchingPositions != null) {
      mdTagsExtant += 1
      if (mdTag.toString != read.getMismatchingPositions) {
        incorrectMDTags += 1
        if (overwriteExistingTags) {
          read.setMismatchingPositions(mdTag.toString)
        } else {
          val exception = IncorrectMDTagException(read, mdTag.toString)
          if (validationStringency == ValidationStringency.STRICT) {
            throw exception
          } else if (validationStringency == ValidationStringency.LENIENT) {
            log.warn(exception.getMessage)
          }
        }
      }
    } else {
      read.setMismatchingPositions(mdTag.toString)
      mdTagsAdded += 1
    }
    read
  }

  def addMDTagsBroadcast(): RDD[AlignmentRecord] = {
    val referenceFileB = sc.broadcast(referenceFile)
    reads.map(read => {
      (for {
        contig <- Option(read.getContigName)
        if read.getReadMapped
      } yield {
        try {
          maybeMDTagRead(read, referenceFileB.value
            .extract(ReferenceRegion.unstranded(read)))
        } catch {
          case t: Throwable => {
            if (validationStringency == ValidationStringency.STRICT) {
              throw t
            } else if (validationStringency == ValidationStringency.LENIENT) {
              log.warn("Caught exception when processing read %s: %s".format(
                read.getContigName, t))
            }
            read
          }
        }
      }).getOrElse({
        numUnmappedReads += 1
        read
      })
    })
  }
}

/**
 * A class describing an exception where a read's MD tag was recomputed and did
 * not match the MD tag originally attached to the read.
 *
 * @param read The read whose MD tag was recomputed, with original MD tag.
 * @param mdTag The recomputed MD tag.
 */
case class IncorrectMDTagException(read: AlignmentRecord, mdTag: String) extends Exception {
  override def getMessage: String =
    s"Read: ${read.getReadName}, pos: ${read.getContigName}:${read.getStart}, cigar: ${read.getCigar}, existing MD tag: ${read.getMismatchingPositions}, correct MD tag: $mdTag"
}
