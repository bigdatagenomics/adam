package org.bdgenomics.adam.models

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.spark.Logging
import Ordering.Option

object ReferencePositionPair extends Logging {
  def apply(singleReadBucket: SingleReadBucket): ReferencePositionPair = {
    singleReadBucket.primaryMapped.lift(0) match {
      case None =>
        // No mapped reads
        new ReferencePositionPair(None, None)
      case Some(read1) =>
        val read1pos = ReferencePositionWithOrientation.fivePrime(read1)
        if (read1.getReadPaired && read1.getMateMapped) {
          singleReadBucket.primaryMapped.lift(1) match {
            case None =>
              // Orphaned read. Missing its mate.
              log.warn("%s denoted mate as mapped but mate does not exist".format(read1.getReadName))
              new ReferencePositionPair(read1pos, None)
            case Some(read2) =>
              // Both reads are mapped
              val read2pos = ReferencePositionWithOrientation.fivePrime(read2)
              if (read1pos < read2pos) {
                new ReferencePositionPair(read1pos, read2pos)
              } else {
                new ReferencePositionPair(read2pos, read1pos)
              }
          }
        } else {
          singleReadBucket.primaryMapped.lift(1) match {
            case None =>
              // Mate is not mapped...
              new ReferencePositionPair(read1pos, None)
            case Some(read2) =>
              val read2pos = ReferencePositionWithOrientation.fivePrime(read2)
              log.warn("%s claimed to not have mate but mate found".format(read1.getReadName))
              if (read1pos < read2pos) {
                new ReferencePositionPair(read1pos, read2pos)
              } else {
                new ReferencePositionPair(read2pos, read1pos)
              }
          }
        }
    }
  }
}

case class ReferencePositionPair(read1refPos: Option[ReferencePositionWithOrientation],
  read2refPos: Option[ReferencePositionWithOrientation])

class ReferencePositionPairSerializer extends Serializer[ReferencePositionPair] {
  val rps = new ReferencePositionWithOrientationSerializer()

  def writeOptionalReferencePos(kryo: Kryo, output: Output, optRefPos: Option[ReferencePositionWithOrientation]) = {
    optRefPos match {
      case None =>
        output.writeBoolean(false)
      case Some(refPos) =>
        output.writeBoolean(true)
        rps.write(kryo, output, refPos)
    }
  }

  def readOptionalReferencePos(kryo: Kryo, input: Input): Option[ReferencePositionWithOrientation] = {
    val exists = input.readBoolean()
    if (exists) {
      Some(rps.read(kryo, input, classOf[ReferencePositionWithOrientation]))
    } else {
      None
    }
  }

  def write(kryo: Kryo, output: Output, obj: ReferencePositionPair) = {
    writeOptionalReferencePos(kryo, output, obj.read1refPos)
    writeOptionalReferencePos(kryo, output, obj.read2refPos)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePositionPair]): ReferencePositionPair = {
    val read1ref = readOptionalReferencePos(kryo, input)
    val read2ref = readOptionalReferencePos(kryo, input)
    new ReferencePositionPair(read1ref, read2ref)
  }
}

