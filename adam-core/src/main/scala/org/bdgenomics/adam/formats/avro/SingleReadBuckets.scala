package org.bdgenomics.adam.formats.avro

import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment, SingleReadBucket }

import scala.collection.JavaConversions._

object SingleReadBuckets {

  def allReads(bucket: SingleReadBucket): Iterable[AlignmentRecord] = {
    bucket.getPrimaryMapped ++ bucket.getSecondaryMapped ++ bucket.getUnmapped
  }

  def toFragment(bucket: SingleReadBucket): Fragment = {
    // take union of all reads, as we will need this for building and
    // want to pay the cost exactly once
    val unionReads = allReads(bucket)

    // start building fragment
    val builder = Fragment.newBuilder()
      .setReadName(unionReads.head.getReadName)
      .setAlignments(seqAsJavaList(unionReads.toSeq))

    // is an insert size defined for this fragment?
    bucket.getPrimaryMapped.headOption
      .foreach(r => {
        Option(r.getInferredInsertSize).foreach(is => {
          builder.setFragmentSize(is.toInt)
        })
      })

    // set record group name, if known
    Option(unionReads.head.getRecordGroupName)
      .foreach(n => builder.setRunId(n))

    builder.build()
  }
}
