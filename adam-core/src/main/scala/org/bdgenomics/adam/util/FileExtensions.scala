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

/**
 * Match path names to file extensions.
 */
object FileExtensions {

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a 2bit format file extension.
   */
  def is2BitExt(pathName: String): Boolean = {
    pathName.endsWith(".2bit")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a BAM/CRAM/SAM format file extension.
   */
  def isBamExt(pathName: String): Boolean = {
    pathName.endsWith(".bam") ||
      pathName.endsWith(".cram") ||
      pathName.endsWith(".sam")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a BED6/12 format file extension.
   */
  def isBedExt(pathName: String): Boolean = {
    pathName.endsWith(".bed")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a FASTA format file extension.
   */
  def isFastaExt(pathName: String): Boolean = {
    pathName.endsWith(".fa") ||
      pathName.endsWith(".fasta")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a FASTQ format file extension.
   */
  def isFastqExt(pathName: String): Boolean = {
    pathName.endsWith(".fq") ||
      pathName.endsWith(".fastq")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a GFF3 format file extension.
   */
  def isGff3Ext(pathName: String): Boolean = {
    pathName.endsWith(".gff3")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a GFF2/GTF format file extension.
   */
  def isGtfExt(pathName: String): Boolean = {
    pathName.endsWith(".gff") ||
      pathName.endsWith(".gtf")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches an interleaved FASTQ format file extension.
   */
  def isInterleavedFastqExt(pathName: String): Boolean = {
    pathName.endsWith(".ifq")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches an IntervalList format file extension.
   */
  def isIntervalListExt(pathName: String): Boolean = {
    pathName.endsWith(".interval_list")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a NarrowPeak format file extension.
   */
  def isNarrowPeakExt(pathName: String): Boolean = {
    pathName.endsWith(".narrowpeak") ||
      pathName.endsWith(".narrowPeak")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a VCF format file extension.
   */
  def isVcfExt(pathName: String): Boolean = {
    pathName.endsWith(".vcf") ||
      pathName.endsWith(".vcf.gz") ||
      pathName.endsWith(".vcf.bgz")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a GZIP format file extension.
   */
  def isGzip(pathName: String): Boolean = {
    pathName.endsWith(".gz") ||
      pathName.endsWith(".bgz")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches an HTSJDK sequence dictionary (.dict) extension.
   */
  def isDictExt(pathName: String): Boolean = {
    pathName.endsWith(".dict")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a Bedtools genome file (.genome) extension.
   */
  def isGenomeExt(pathName: String): Boolean = {
    pathName.endsWith(".genome") ||
      pathName.endsWith(".genome.txt")
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a text file (.txt) extension.
   */
  def isTextExt(pathName: String): Boolean = {
    pathName.endsWith(".txt")
  }
}
