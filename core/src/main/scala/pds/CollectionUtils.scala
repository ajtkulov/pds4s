package pds

import scala.collection.mutable.ArrayBuffer

case class StringCounter(counter: Map[String, Int], length: Int) {}

object CollectionUtils {
  implicit class KeyInSet[K, V](key: K) {
    implicit def in(set: Set[K]): Boolean = {
      set.contains(key)
    }

    implicit def in(map: Map[K, V]): Boolean = {
      map.contains(key)
    }
  }

  implicit class MapPlus[K, V](fst: Map[K, V]) {
    def +(snd: Map[K, V])(implicit t: Numeric[V]): Map[K, V] = {
      val keys: Set[K] = fst.keySet ++ snd.keySet
      keys.map(key => {
        val fstValue: V = fst.getOrElse(key, t.zero)
        val sndValue: V = snd.getOrElse(key, t.zero)
        (key, t.plus(fstValue, sndValue))
      }).toMap
    }
  }

  def findPrev[T](values: List[T], item: T): Option[T] = {
    values.zipWithIndex.find(_._1 == item).flatMap {
      case (_, idx) => if (idx > 0) {
        Some(values(idx - 1))
      } else {
        None
      }
    }
  }

  def findNext[T](values: List[T], item: T): Option[T] = {
    values.zipWithIndex.find(_._1 == item).flatMap {
      case (_, idx) => if (idx < values.length - 1) {
        Some(values(idx + 1))
      } else {
        None
      }
    }
  }

  implicit class Aggregate[T](values: List[T]) {
    implicit def minByWithDefault[N: Numeric](f: T => N): Option[T] = {
      if (values.isEmpty) {
        None
      } else {
        Some(values.minBy(f))
      }
    }
  }

  implicit class FilterBy[T](values: List[T]) {
    implicit def filterBy(p: T => Boolean): (List[T], List[T]) = {
      (values.filter(p), values.filterNot(p))
    }
  }

  def interning[T](values: List[T], dict: Map[T, Int]): (List[Int], Map[T, Int]) = {
    val d = scala.collection.mutable.Map[T, Int]() ++ dict

    val list = values.map(item =>
      if (d.contains(item)) {
        d(item)
      } else {
        val res = d.size
        d(item) = d.size
        res
      }
    )

    (list, d.toMap)
  }

  def grouping[T](sorted: List[T], sameGroupFunc: (T, T) => Boolean): List[List[T]] = {
    if (sorted.isEmpty) {
      List()
    } else {
      val res = ArrayBuffer[ArrayBuffer[T]]()

      res.append(ArrayBuffer())
      res.last.append(sorted.head)
      for (item <- sorted.drop(1)) {
        if (sameGroupFunc(res.last.last, item)) {
          res.last.append(item)
        } else {
          res.append(ArrayBuffer())
          res.last.append(item)
        }
      }

      res.map(_.toList).toList
    }
  }
}
