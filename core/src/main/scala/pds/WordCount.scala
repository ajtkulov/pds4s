package pds

import com.clearspring.analytics.stream.frequency.{CountMinSketch, TinyCountMinSketch}

trait WordCount {
  def count(word: String): Int
}

object FakeWordCount extends WordCount {
  override def count(word: String): Int = 0
}

case class MapWordCount(values: Map[String, Int]) extends WordCount {
  def count(word: String): Int = values.getOrElse(word, 0)

  lazy val totalDoc: Int = values.keySet.size
  lazy val totalItems: Int = values.values.sum
}

case class CountMinSketchWordCount(values: CountMinSketch) extends WordCount {
  def count(word: String): Int = values.estimateCount(word).toInt
}

case class TinyCountMinSketchWordCount(values: TinyCountMinSketch) extends WordCount {
  def count(word: String): Int = values.estimateCount(word).toInt
}
