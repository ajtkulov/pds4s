package pds.ds

import com.clearspring.analytics.stream.membership.{BigBloomFilter, BigBloomFilterSerializer}
import pds.FileUtils

import java.io._

trait BloomFilterComposite {
  def put(key: String): Boolean

  def isPresent(key: String): Boolean
}

case class WrapBloomFilter(origin: BigBloomFilter) extends BloomFilterComposite {
  override def put(key: String): Boolean = origin.put(key)

  override def isPresent(key: String): Boolean = origin.isPresent(key)
}

case class BloomFilterCompound(filters: Array[BigBloomFilter]) extends BloomFilterComposite {
  def bucket(key: String): Int = {
    val abs = Math.abs(key.hashCode)
    // This case only for value Int.MinValue
    if (abs < 0) {
      0
    } else {
      abs % filters.length
    }
  }

  override def put(key: String): Boolean = filters(bucket(key)).put(key)

  override def isPresent(key: String): Boolean = filters(bucket(key)).isPresent(key)
}

object BloomFilterComposite {
  val chunkSize: Long = 1000000000L

  def create(numElements: Long, bucketsPerElement: Int, numHashes: Int): BloomFilterComposite = {
    if (numElements < chunkSize) {
      WrapBloomFilter(new BigBloomFilter(numElements, bucketsPerElement, numHashes))
    } else {
      val chunks = (numElements / chunkSize + 1).toInt
      BloomFilterCompound(Array.fill(chunks)(new BigBloomFilter(numElements / chunks, bucketsPerElement, numHashes)))
    }
  }

  def deserialize(prefixName: String): BloomFilterComposite = {
    val ser = new BigBloomFilterSerializer()
    if (FileUtils.exist(s"$prefixName.s.bloom")) {
      val single = ser.deserialize(new DataInputStream(new FileInputStream(new File(s"$prefixName.s.bloom"))))
      WrapBloomFilter(single)
    } else {
      val list = for (idx <- Iterator.iterate(0)(_ + 1).take(100) if FileUtils.exist(s"$prefixName.$idx.bloom")) yield {
        ser.deserialize(new DataInputStream(new FileInputStream(new File(s"$prefixName.$idx.bloom"))))
      }
      BloomFilterCompound(list.toArray)
    }
  }

  implicit class Serializer(value: BloomFilterComposite) {
    val ser = new BigBloomFilterSerializer()

    implicit def serialize(prefixName: String): Unit = {
      value match {
        case WrapBloomFilter(s) => ser.serialize(s, new DataOutputStream(new FileOutputStream(new File(s"$prefixName.s.bloom"))))
        case BloomFilterCompound(ar) => ar.zipWithIndex.foreach { case (b, idx) => ser.serialize(b, new DataOutputStream(new FileOutputStream(new File(s"$prefixName.$idx.bloom")))) }
      }
    }
  }

}
