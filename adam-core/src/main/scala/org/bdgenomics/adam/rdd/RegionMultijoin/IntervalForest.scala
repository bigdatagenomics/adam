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
package org.bdgenomics.adam.rdd.regionMultiJoin

import org.bdgenomics.adam.models.ReferenceRegion
import scala.annotation.tailrec

class IntervalForest[T](allRegions: List[(ReferenceRegion, T)]) extends Serializable {
  private val chromosomes = allRegions.collect { case x => x._1.referenceName }.distinct //get all chromosome names and build a tree for each chromosome
  private val intervalTrees = chromosomes.map(x => {
    val sameChrRegions = allRegions.filter(_._1.referenceName.equals(x))
    (x, new Node(sameChrRegions))
  }).toMap

  def getAllOverlappings(r: ReferenceRegion) = {
    val root = intervalTrees.get(r.referenceName)
    root match {
      case None     => Nil
      case Some(rt) => allOverlappingRegions(r, rt)
    }

  }

  /*Return the number of tree nodes for the input chromosome*/
  def getTreeNodeCount(chr: String) = {
    val root = intervalTrees.get(chr)
    root match {
      case None     => 0
      case Some(rt) => countNodes(rt)
    }
  }

  /*Return the height of the tree of a particular chromosome*/
  def getTreeHeight(chr: String) = {
    val root = intervalTrees.get(chr)
    root match {
      case None     => 0
      case Some(rt) => treeHeight(rt)
    }
  }

  private def allOverlappingRegions(r: ReferenceRegion, rt: Node): List[(ReferenceRegion, T)] = {
    if (rt == null)
      return Nil
    val resultFromThisNode = r match {
      case x if (rt.inclusiveIntervals == Nil) => Nil //Sometimes a node can have zero intervals
      case x if (x.end < rt.minPointOfCollection || x.start > rt.maxPointOfCollection) => Nil //Save unnecessary filtering
      case _ => rt.inclusiveIntervals.filter(t => r.overlaps(t._1))
    }

    if (r.overlaps(ReferenceRegion(r.referenceName, rt.centerPoint, rt.centerPoint + 1)))
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild) ++ allOverlappingRegions(r, rt.rightChild)
    else if (r.end <= rt.centerPoint)
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild)
    else if (r.start > rt.centerPoint)
      return resultFromThisNode ++ allOverlappingRegions(r, rt.rightChild)
    else throw new NoSuchElementException("Interval Tree Exception. Illegal comparison for centerpoint " + rt.centerPoint)

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
        if (x._1.end < centerPoint) leftRegions ::= x
        else if (x._1.start > centerPoint) rightRegions ::= x
        else centerRegions ::= x
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

  private def countNodes(root: Node): Int = {
    root match {
      case null => 0
      case _    => 1 + countNodes(root.leftChild) + countNodes(root.rightChild)
    }
  }

  private def treeHeight(root: Node): Int = {
    root match {
      case null => -1
      case _    => 1 + math.max(treeHeight(root.leftChild), treeHeight(root.rightChild))
    }
  }

}
