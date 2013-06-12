package edu.berkeley.cs.amplab.adam.avro

import java.io.{ObjectInputStream, ObjectOutputStream, IOException, Serializable}
import org.apache.avro.Schema
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter, SpecificDatumReader}

object AvroWrapper {

  // Cache of reader, writer, uniqueid
  private val writerCache = scala.collection.mutable.Map[Class[_], (DatumWriter[_], Int)]()
  private val readerCache = scala.collection.mutable.Map[Int, DatumReader[_]]()
  private var uniqueId = 1

  implicit def apply[T <: SpecificRecord : Manifest](avro: T) = {
    new AvroWrapper[T](avro)
  }

  implicit def unwrap[T](wrapper: AvroWrapper[T]) = wrapper.obj

  def serialize[T <% SpecificRecord](t: T, encoder: Encoder) = {
    val (writer, uniqueId) = writerCache(t.getClass.asInstanceOf[Class[T]])
    // Write the unique ID
    encoder.writeInt(uniqueId)
    // Write the object
    writer.asInstanceOf[DatumWriter[T]].write(t, encoder)
  }

  def deserialize[T <% SpecificRecord](decoder: Decoder) = {
    // Read the unique ID
    val uniqueId = decoder.readInt()
    // Use the ID to get the correct reader
    val r = readerCache(uniqueId).asInstanceOf[DatumReader[T]]
    r.read(null.asInstanceOf[T], decoder)
  }

  def register[T <% SpecificRecord](klazz: Class[T]) = {
    writerCache(klazz) = (new SpecificDatumWriter[T](klazz), uniqueId)
    readerCache(uniqueId) = new SpecificDatumReader[T](klazz)
    uniqueId += 1
  }

}

class AvroWrapper[T <% SpecificRecord : Manifest](private var obj: T)
  extends Serializable with SpecificRecord {
  var encoder: BinaryEncoder = null
  var decoder: BinaryDecoder = null

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    encoder = EncoderFactory.get().binaryEncoder(out, encoder)
    AvroWrapper.serialize[T](obj, encoder)
    encoder.flush()
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    decoder = DecoderFactory.get().binaryDecoder(in, decoder)
    obj = AvroWrapper.deserialize[T](decoder)
  }

  def put(i: Int, v: scala.Any) {
    obj.put(i, v)
  }

  def get(i: Int): AnyRef = obj.get(i)

  def getSchema: Schema = obj.getSchema
}

