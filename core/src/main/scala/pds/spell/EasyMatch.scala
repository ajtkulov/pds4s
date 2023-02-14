package pds.spell

import pds.{Strategy, ds}
import Match._

trait EasyMatch {
  def uni: TrieStat

  implicit def strategy: Strategy

  def delete(value: String): List[String] = {
    val res = for {i <- 0 until value.length
                   deleted = value.substring(0, i) + value.substring(i + 1)
                   if strategy.acceptDelete(ds.Error.empty.copy(positionInWord = i)) && uni.trie.exactMatch(deleted)
                   } yield deleted
    res.toList
  }

  def swap(value: String): List[String] = {
    val res = for {i <- 0 until value.length - 1
                   swap = value.substring(0, i) + value(i + 1) + value(i) + value.substring(i + 2)
                   if strategy.acceptSwap(ds.Error.empty.copy(positionInWord = i)) && uni.trie.exactMatch(swap)
                   } yield swap
    res.toList
  }

  def simpleCheck(name: List[String]): Boolean = {
    name.count(w => !uni.trie.exactMatch(w)) == 1
  }

  def replace(name: List[String], bad: String, options: List[String]): Result = {
    for {
      change <- options
      idx <- name.zipWithIndex.find {
        case (v, _) => v == bad
      }
    } yield {
      FuzzyMatch(name.updated(idx._2, change))
    }
  }

  def replaceFlatMap(name: List[String], bad: String, options: List[List[String]]): Result = {
    for {
      change <- options
      idx <- name.zipWithIndex.find {
        case (v, _) => v == bad
      }
      (head, tail) = name.splitAt(idx._2)
    } yield {
      //Better way, parameterize ctor call by factory and parameter
      Split(head.filterNot(_ == bad) ++ change ++ tail.filterNot(_ == bad))
    }
  }

  def simple(name: List[String]): Result = {
    if (simpleCheck(name)) {
      val bad = name.find(w => !uni.trie.exactMatch(w)).get
      val options = swap(bad) ++ delete(bad)
      replace(name, bad, options)
    } else {
      List()
    }
  }
}
