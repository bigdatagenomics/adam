/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.util

import java.io.{FileInputStream, File, InputStream}

import scala.io._
import edu.berkeley.cs.amplab.adam.avro.ADAMCall
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary}

/**
 * My own (tdanford) utility class for parsing VCF files.
 *
 */
object VCFReader {

  val emptyDict = SequenceDictionary()

  def apply(file : File, seqDict : SequenceDictionary = emptyDict) : Traversable[ADAMCall] =
    this(new FileInputStream(file), seqDict)

  /**
   * On-demand parsing of the VCF, without reading the whole file into memory first.
   *
   * @param is An InputStream whose contents are a (UTF-8 encoded) VCF file.
   * @return
   */
  def apply(is : InputStream, seqDict : SequenceDictionary) : Traversable[ADAMCall] = {
    var dict = seqDict
    val itr : Iterator[String] = Source.fromInputStream(is).getLines()
    val samples = itr.find(_.startsWith("#CHROM")).get.split("\t").drop(9)

    new Traversable[ADAMCall] {
      def foreach[U](f: (ADAMCall) => U): Unit = {
        itr.foreach {
          line =>
            val array : Array[String] = line.split("\t")

            // parse out the site-level data.
            val referenceName = array(0)
            val start = array(1).toInt
            val siteId : Option[String] = if(!array(2).startsWith(".")) Some(array(2)) else None
            val referenceAllele = array(3)
            val altAlleles : Array[String] = array(4).split(",")
            val siteQuality = array(5).toDouble
            val siteFilter = array(6)
            val siteInfo = array(7)
            val format = array(8)

            if(!dict.hasSequence(referenceName)) {
              // TODO: fix this length=1 thing.
              dict = dict + SequenceRecord(seqDict.nonoverlappingHash(referenceName), referenceName, 1L)
            }

            val referenceId = dict(referenceName).id
            val referenceLength = dict(referenceName).length
            val referenceUrl = dict(referenceName).url

            // needed for the 0-based indexing of each genotype call into the set of alleles, below.
            val alleleArray : Array[String] = referenceAllele +: altAlleles

            array.drop(9).zip(samples).foreach {
              case (vcfEntry, sampleId) => {

                // ignore "missing" entries.
                if(vcfEntry != "./.") {

                  // this implementation only does unphased entries, and assumes that the format
                  // field is fixed (and doesn't pay attention to the 'format' value, above).
                  val elmts = vcfEntry.split(":")
                  val alleles = elmts(0).split("/").map( idxString => alleleArray(idxString.toInt))
                  val alleleDepth = elmts(1)
                  val depth : Int = elmts(2).toInt
                  val genotypeQuality = elmts(3).toDouble
                  val likelihoods = elmts(4)

                  val call = ADAMCall.newBuilder()
                    .setReferenceName(referenceName)
                    .setReferenceId(referenceId)
                    .setReferenceLength(referenceLength)
                    .setReferenceUrl(referenceUrl)
                    .setStart(start)
                    .setSiteId(siteId.getOrElse(null))
                    .setSiteFilter(siteFilter)
                    .setReferenceAllele(referenceAllele)
                    .setSampleId(sampleId)
                    .setAllele1(alleles(0))
                    .setAllele2(alleles(1))
                    .setSiteQuality(siteQuality)
                    .setDepth(depth)
                    .setGenotypeQuality(genotypeQuality)
                    .setIsPhased(false)
                    .build()

                  f(call)
                }
              }

            }
        }

      }
    }
  }
}

class Call()
