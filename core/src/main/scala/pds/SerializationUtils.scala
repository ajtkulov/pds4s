package pds

import pds.lsh.MinHash

import java.io._
import scala.io.Codec

/**
 * Utility methods for serialization/deserialization
 */
object SerializationUtils {

  def serialize(obj: Any, outputStream: OutputStream): Unit = {
    val out = new ObjectOutputStream(outputStream)
    out.writeObject(obj)
    out.close()
  }

  def serialize(obj: Any, fileName: String): Unit = {
    serialize(obj, new FileOutputStream(new File(fileName)))
  }

  def serialize(obj: Any): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    serialize(obj, stream)
    stream.toByteArray
  }

  def deserialize[T](inputStream: InputStream): T = {
    val objReader = new ObjectInputStream(inputStream)
    objReader.readObject().asInstanceOf[T]
  }

  def deserializeFromFile[T](fileName: String): T = {
    deserialize[T](new FileInputStream(new File(fileName)))
  }

  def getUid[T](value: T): Long = {
    ObjectStreamClass.lookup(value.getClass).getSerialVersionUID
  }

  def bitsCount(fileName: String): Long = {
    var res = 0L
    val arr = new Array[Char](100)
    val reader = scala.io.Source.fromFile(fileName)(Codec.ISO8859).bufferedReader()
    while (reader.read(arr) != -1) {
      for (ch <- arr) {
        res += MinHash.bitsCount(ch)
      }
    }

    res
  }

  def cache[T](fileName: String)(block: => T): T = {
    if (FileUtils.exist(fileName)) {
      deserialize[T](new DataInputStream(new FileInputStream(new File(fileName))))
    } else {
      val value: T = block
      SerializationUtils.serialize(value, new FileOutputStream(new File(fileName)))
      value
    }
  }
}
