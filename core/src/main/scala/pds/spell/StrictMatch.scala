package pds.spell

import pds.CollectionUtils
import pds.ds.{Error, MatchItem}
import pds.spell.Match._

trait StrictMatch extends Match with SplitJoin {
  lazy val stopSet: Set[String] = Set()

  override def matchString(name: String): Result = {
    val split = name.split(" ").toList

    strictSimple(split) ++ cut(split) ++ correction(split) ++ splitJoin(split) ++ List(ExactMatch(name))
  }

  def filterStrategy(previousCnt: Int, newWord: String): Boolean = {
    (previousCnt, uni.count(newWord)) match {
      case (before, after) if before <= 5 => after >= before
      case (before, after) if before <= 50 => after >= before * 5 && after >= 200
      case (before, after) if before <= 200 => after >= before * 5
      case _ => false
    }
  }

  def strictSimple(name: List[String]): Result = {
    findToCorrect(name).map { case (bad, cnt) =>
      val options: List[String] = swap(bad) ++ delete(bad)
      replace(name, bad, options.filter(word => filterStrategy(cnt, word)))
    }.getOrElse(List())
  }

  def findToCorrect(name: List[String]): Option[(String, Int)] = {
    if (name.count(w => uni.count(w) <= 5) == 1) {
      name.find(w => uni.count(w) <= 5).map(x => (x, uni.count(x)))
    } else {
      val min = name.minBy(w => uni.count(w))
      val freq = uni.count(min)
      if (freq <= 200) {
        Some(min -> freq)
      } else {
        None
      }
    }
  }

  def cutUseLess(split: List[String]): Boolean = {
    split match {
      case _ :+ last if uni.count(last) >= 50 && last.length >= 5 => true
      case _ :+ _ :+ last if last.length >= 3 && stopSet.contains(last) => true
      case _ :+ penultimate :+ last if pairs.count(s"$penultimate $last") >= 200 => true
      case _ => false
    }
  }

  def cut(split: List[String]): Result = {
    if (cutUseLess(split)) {
      List()
    } else {
      val items: List[MatchItem] = uni.trie.fuzzyMatchContItems(split.last, 0)
      val words: List[String] = items.map(x => (x, uni.count(x.value))).filter(_._2 >= uni.count(split.last) * 0.8)
        .sortBy(_._2)(Ordering[Int].reverse).map(_._1.value)

      val res = if (split.size > 1) {
        val pred = split.dropRight(1).last
        val pairCut: List[String] = pairs.trie.fuzzyMatchContItems(s"$pred ${split.last}", 0).map(x => (x, pairs.count(x.value)))
          .sortBy(_._2)(Ordering[Int].reverse).map(_._1.value.split(" ").last)
        if (pairCut.isEmpty) {
          words
        } else {
          pairCut
        }
      } else {
        words
      }

      res.take(5).map(x => CutMatch(split.dropRight(1) :+ x, Error.empty))
    }
  }

  def correction(split: List[String]): Result = {
    findToCorrect(split).map { case (bad, cnt) =>
      val items = uni.trie.fuzzyMatchItems(bad, maxErrors)

      val centralWords = items.map(x => (x, uni.count(x.value))).sortBy(_._2)(Ordering[Int].reverse).take(5).map(_._1).map(_.value)

      val prev = CollectionUtils.findPrev(split, bad).toList.flatMap { prev =>
        pairs.trie.fuzzyMatchItems(s"$prev $bad", maxErrors).filter(_.value.startsWith(prev)).map(x => (pairs.count(x.value), x.value.split(" ").last))
      }

      val next = CollectionUtils.findNext(split, bad).toList.flatMap { next =>
        pairs.trie.fuzzyMatchItems(s"$bad $next", maxErrors).filter(_.value.endsWith(next)).map(x => (pairs.count(x.value), x.value.split(" ").head))
      }

      val byPair = (prev ++ next).sortBy(_._1)(Ordering[Int].reverse).map(_._2).take(5)

      val res: List[String] = if (byPair.nonEmpty) {
        byPair
      } else {
        centralWords
      }

      replace(split, bad, res.filter(word => filterStrategy(cnt, word)))
    }.getOrElse(List())
  }

  def splitJoin(split: List[String]): Result = {
    if (split.size == 1 && split.head.length >= 8 && uni.count(split.head) <= 5) {
      smartSplit(split.head)
    } else {
      findToCorrect(split).toList.flatMap { case (bad, cnt) =>
        if (bad.length >= 7 && cnt <= 5) {
          val res = smartSplit(bad).filter(_.split.size <= 3).map(_.split)
          replaceFlatMap(split, bad, res)
        } else {
          List()
        }
      }
    }
  }
}
