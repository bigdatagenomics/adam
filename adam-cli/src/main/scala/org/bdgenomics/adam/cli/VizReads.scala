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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ OrderedTrackedLayout, ReferenceRegion }
import org.bdgenomics.adam.projections.AlignmentRecordField._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.ReferenceMappingContext.AlignmentRecordReferenceMapping
import org.bdgenomics.formats.avro.AlignmentRecord
import org.fusesource.scalate.TemplateEngine
import org.json4s._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.scalatra.json._
import org.scalatra.ScalatraServlet

object VizReads extends ADAMCommandCompanion {
  val commandName: String = "viz"
  val commandDescription: String = "Generates images from sections of the genome"

  var refName = ""
  var inputPath = ""
  var reads: RDD[AlignmentRecord] = null

  val trackHeight = 5
  val width = 1200
  val base = 20

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new VizReads(Args4j[VizReadsArgs](cmdLine))
  }

  def printJson(layout: OrderedTrackedLayout[AlignmentRecord]): List[TrackJson] = {
    var tracks = new scala.collection.mutable.ListBuffer[TrackJson]

    // draws a box for each read, in the appropriate track.
    for ((rec, track) <- layout.trackAssignments) {
      val aRec = rec.asInstanceOf[AlignmentRecord]
      tracks += new TrackJson(aRec.getReadName, aRec.getStart, aRec.getEnd, track)
    }
    tracks.toList
  }
}

case class TrackJson(readName: String, start: Long, end: Long, track: Long)

class VizReadsArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM Records file to view", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "REFNAME", usage = "The reference to view", index = 1)
  var refName: String = null
}

class VizServlet extends ScalatraServlet with JacksonJsonSupport {
  protected implicit val jsonFormats: Formats = DefaultFormats
  var regInfo = ReferenceRegion(VizReads.refName, 0, 100)
  var filteredLayout: OrderedTrackedLayout[AlignmentRecord] = null

  get("/?") {
    redirect(url("reads"))
  }

  get("/reads/?") {
    contentType = "text/html"

    filteredLayout = new OrderedTrackedLayout(VizReads.reads.filterByOverlappingRegion(regInfo).collect())
    val templateEngine = new TemplateEngine
    templateEngine.layout("adam-cli/src/main/webapp/WEB-INF/layouts/default.ssp",
      Map("regInfo" -> (regInfo.referenceName, regInfo.start.toString, regInfo.end.toString),
        "width" -> VizReads.width.toString,
        "base" -> VizReads.base.toString,
        "numTracks" -> filteredLayout.numTracks.toString,
        "trackHeight" -> VizReads.trackHeight.toString))
  }

  get("/reads/:ref") {
    contentType = formats("json")

    regInfo = ReferenceRegion(params("ref"), params("start").toLong, params("end").toLong)
    filteredLayout = new OrderedTrackedLayout(VizReads.reads.filterByOverlappingRegion(regInfo).collect())
    VizReads.printJson(filteredLayout)
  }
}

class VizReads(protected val args: VizReadsArgs) extends ADAMSparkCommand[VizReadsArgs] {
  val companion: ADAMCommandCompanion = VizReads

  def run(sc: SparkContext, job: Job): Unit = {
    VizReads.refName = args.refName

    val proj = Projection(contig, readMapped, readName, start, end)
    VizReads.reads = sc.adamLoad(args.inputPath, projection = Some(proj))

    val server = new org.eclipse.jetty.server.Server(8080)
    val handlers = new org.eclipse.jetty.server.handler.ContextHandlerCollection()
    server.setHandler(handlers)
    handlers.addHandler(new org.eclipse.jetty.webapp.WebAppContext("adam-cli/src/main/webapp", "/"))
    server.start()
    println("View the visualization at: 8080")
    server.join()
  }

}