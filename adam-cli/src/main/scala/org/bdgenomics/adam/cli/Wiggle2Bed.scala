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

package org.bdgenomics.adam.cli

import java.io.PrintWriter
import org.kohsuke.args4j.Option
import scala.io.Source

class Wig2BedArgs extends Args4jBase {
  @Option(name = "-wig", usage = "The wig file to convert (leave out for stdin)")
  var wigPath: String = ""

  @Option(name = "-bed", usage = "Location to write BED data (leave out for stdout)")
  var bedPath: String = ""
}

/**
 * WigFix2Bed (accessible as the command "wigfix2bed" through the CLI) takes
 * fixed wiggle file and converts it to a BED formatted file.  The wiggle file
 * is a text based format that implements run-length encoding, without any
 * guarantees where the sync markers are.  This makes it difficult to use as a
 * "splittable" format, and necessitates processing the file locally.
 */
object WigFix2Bed extends ADAMCommandCompanion {
  val commandName = "wigfix2bed"
  val commandDescription = "Locally convert a wigFix file to BED format"

  // matches a "sync" line that resets the position
  val declPattern = "^fixedStep[\\s]+chrom=(.+)[\\s]+start=([0-9]+)[\\s]+step=([0-9]+)[\\s]*(?:$|span=([0-9]+).*$)".r
  // a single datum in the run-length encoded file
  val featPattern = "^\\s*([-]?[0-9]*\\.?[0-9]*)\\s*$".r

  def apply(cmdLine: Array[String]) = {
    new WigFix2Bed(Args4j[Wig2BedArgs](cmdLine))
  }
}

class WigFix2Bed(val args: Wig2BedArgs) extends ADAMCommand {
  val companion = WigFix2Bed

  def run() {
    // state from the declaration lines
    var contig: String = ""
    var current: Long = 0
    var step: Long = 0
    var span: Long = 1

    val in = if (args.wigPath == "") Source.stdin else Source.fromFile(args.wigPath)
    val out = if (args.bedPath == "") new PrintWriter(System.out) else new PrintWriter(args.bedPath)
    in.getLines().foreach {
      case WigFix2Bed.declPattern(c, st, sp, sn) => {
        contig = c
        current = st.toLong - 1 // convert to BED coords
        step = sp.toLong
        span = if (sn == null) span else sn.toLong
      }
      case WigFix2Bed.featPattern(value) => {
        out.println(Array(contig, current.toString, (current + span).toString, "", value).mkString("\t"))
        current += step
      }
      case _ => None
    }
    in.close()
    out.close()
  }
}
