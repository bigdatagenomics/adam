/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.rdd.RegionMultijoin

import org.bdgenomics.adam.models.ReferenceRegion

class IntervalTree[T](chr: String, allRegions: List[(ReferenceRegion, T)]) extends Serializable {
  val chromosome = chr

  //for now let's require that allRegions should come from the same chromosome
  assert(allSameChromosome(allRegions))
  val root = new Node(allRegions)

  def allSameChromosome(regions: List[(ReferenceRegion, T)]): Boolean = regions match {
    case Nil => true
    case h :: tail if (h._1.referenceName.equals(chromosome)) => allSameChromosome(tail)
    case _ => false
  }

  def getAllOverlappings(r: ReferenceRegion) = allOverlappingRegions(r, root)

  def allOverlappingRegions(r: ReferenceRegion, rt: Node): List[(ReferenceRegion, T)] = {
    if (!r.referenceName.equals(chromosome))
      return Nil
    if (rt == null)
      return Nil
    val resultFromThisNode = r match {
      case x if (rt.inclusiveIntervals == Nil) => Nil //Sometimes a node can have zero intervals
      case x if (x.end < rt.minPointOfCollection || x.start > rt.maxPointOfCollection) => Nil //Save unnecessary filtering
      case _ => rt.inclusiveIntervals.filter(t => regionOverlap(r, t._1))
    }

    if (regionOverlap(r, ReferenceRegion(chromosome, rt.centerPoint, rt.centerPoint + 1)))
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild) ++ allOverlappingRegions(r, rt.rightChild)
    else if (r.end < rt.centerPoint)
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild)
    else if (r.start > rt.centerPoint)
      return resultFromThisNode ++ allOverlappingRegions(r, rt.rightChild)
    else throw new NoSuchElementException("Interval Tree Exception. Illegal comparison for centerpoint " + rt.centerPoint)

  }

  def regionOverlap(r1: ReferenceRegion, r2: ReferenceRegion) = r1 match {
    case x if (x.referenceName != r2.referenceName) => false
    case x if (x.start >= r2.start && x.start <= r2.end) => true
    case x if (r2.start >= x.start && r2.start <= x.end) => true
    case _ => false

  }

  class Node(allRegions: List[(ReferenceRegion, T)]) {

    private val largestPoint = allRegions.maxBy(_._1.end)._1.end
    private val smallestPoint = allRegions.minBy(_._1.start)._1.start
    val centerPoint = smallestPoint + (largestPoint - smallestPoint) / 2

    val (inclusiveIntervals, leftChild, rightChild) = distributeRegions()
    val minPointOfCollection: Long = inclusiveIntervals match {
      case Nil => -1
      case _   => inclusiveIntervals.minBy(_._1.start)._1.start
    }

    val maxPointOfCollection: Long = inclusiveIntervals match {
      case Nil => -1
      case _   => inclusiveIntervals.maxBy(_._1.end)._1.end
    }

    def distributeRegions() = {
      var leftRegions: List[(ReferenceRegion, T)] = Nil
      var rightRegions: List[(ReferenceRegion, T)] = Nil
      var centerRegions: List[(ReferenceRegion, T)] = Nil

      allRegions.foreach(x => {
        if (x._1.end < centerPoint) leftRegions :::= List(x)
        else if (x._1.start > centerPoint) rightRegions :::= List(x)
        else centerRegions :::= List(x)
      })

      val leftChild: Node = leftRegions match {
        case Nil => null
        case _   => new Node(leftRegions)
      }

      val rightChild: Node = rightRegions match {
        case Nil => null
        case _   => new Node(rightRegions)
      }
      (centerRegions, leftChild, rightChild)
    }

  }

}
