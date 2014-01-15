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
package org.bdgenomics.adam.models

import scala.collection.mutable

/**
 * A TrackedLayout is an assignment of values of some type T (which presumably are mappable to
 * a reference genome or other linear coordinate space) to 'tracks' -- that is, to integers,
 * with the guarantee that no two values assigned to the same track will overlap.
 *
 * This is the kind of data structure which is required for non-overlapping genome visualization.
 *
 * @tparam T the type of value which is to be tracked.
 */
trait TrackedLayout[T] {
  def numTracks: Int
  def trackAssignments: Map[T, Int]
}

object TrackedLayout {

  def overlaps[T](rec1: T, rec2: T)(implicit rm: ReferenceMapping[T]): Boolean = {
    val ref1 = rm.getReferenceRegion(rec1)
    val ref2 = rm.getReferenceRegion(rec2)
    ref1.overlaps(ref2)
  }
}

/**
 * An implementation of TrackedLayout which takes a sequence of ReferenceMappable values,
 * and lays them out <i>in order</i> (i.e. from first-to-last) in the naive way: for each
 * value, it looks for the track with the lowest index that doesn't already have an overlapping
 * value, and it doesn't use any special data structures (it just does a linear search for each
 * track.)
 *
 * @param reads The set of values (i.e. "reads", but anything that is mappable to the reference
 *              genome ultimately) to lay out in tracks
 * @param mapping The (implicit) reference mapping which converts values to ReferenceRegions
 * @tparam T the type of value which is to be tracked.
 */
class OrderedTrackedLayout[T](reads: Traversable[T])(implicit val mapping: ReferenceMapping[T]) extends TrackedLayout[T] {

  private var trackBuilder = new mutable.ListBuffer[Track]()
  reads.toSeq.foreach(findAndAddToTrack)
  trackBuilder = trackBuilder.filter(_.records.nonEmpty)

  val numTracks = trackBuilder.size
  val trackAssignments: Map[T, Int] =
    Map(trackBuilder.toList.zip(0 to numTracks).flatMap {
      case (track: Track, idx: Int) => track.records.map(_ -> idx)
    }: _*)

  private def findAndAddToTrack(rec: T) {
    val reg = mapping.getReferenceRegion(rec)
    if (reg != null) {
      val track: Option[Track] = trackBuilder.find(track => !track.conflicts(rec))
      track.map(_ += rec).getOrElse(addTrack(new Track(rec)))
    }
  }

  private def addTrack(t: Track): Track = {
    trackBuilder += t
    t
  }

  private class Track(val initial: T) {

    val records = new mutable.ListBuffer[T]()
    records += initial

    def +=(rec: T): Track = {
      records += rec
      this
    }

    def conflicts(rec: T): Boolean =
      records.exists(r => TrackedLayout.overlaps(r, rec))
  }

}
