package pds.spell

import pds._
import pds.ds.{MatchItem, Trie}
import pds.spell.Match._

object Match {
  type Result = List[MatchResult]
  implicit val limit: Limit = Limit(100)

  implicit class ExtensionMethod(boolean: Boolean) {
    def toExactMatch(name: String): List[MatchResult] = {
      if (boolean) {
        List(ExactMatch(name))
      } else {
        Nil
      }
    }
  }
}

case class Limit(limit: Int) {}

trait CommonStat {
  def count(word: String): Int

  def count: WordCount
}

object EmptyCommonStat extends CommonStat {
  override def count(word: String): Int = 0

  val count: WordCount = MapWordCount(Map())
}

case class TrieStat(trie: Trie, count: WordCount) extends CommonStat {
  def count(word: String): Int = {
    if (trie.exactMatch(word)) {
      count.count(word)
    } else {
      0
    }
  }
}

case class CmsStat(count: WordCount) extends CommonStat {
  def count(word: String): Int = {
    count.count(word)
  }
}

trait ScoringMatch {
  def uni: CommonStat

  def pairs: CommonStat

  def triple: CommonStat

  def enhanceStats(split: List[String]): StatsMatch = {
    val uno = for (i <- split.indices) yield {
      uni.count(split(i))
    }

    val bi = for (i <- 0 until split.size - 1) yield {
      val s = split(i) + " " + split(i + 1)
      pairs.count(s)
    }

    val triples = for (i <- 0 until split.size - 2) yield {
      val s = split(i) + " " + split(i + 1) + " " + split(i + 2)
      triple.count(s)
    }

    val pair = if (uno.contains(0)) {
      List.fill(bi.size)(0)
    } else {
      bi.toList
    }

    val tri = if (pair.contains(0)) {
      List.fill(triples.size)(0)
    } else {
      triples.toList
    }

    StatsMatch(uno.toList, pair, tri)
  }
}

trait Match extends EasyMatch with ScoringMatch {
  def uni: TrieStat

  def pairs: TrieStat

  def triple: TrieStat

  def maxErrors: Int

  implicit def strategy: Strategy

  def matchString(name: String): Result = {
    val split = name.split(" ").toList

    val res = if (split.size == 1) {
      matchUni(name)
    } else if (split.size == 2) {
      matchPair(name)
    } else {
      matchMulti(split)
    }

    res ++ simple(split) ++ List(ExactMatch(name))
  }

  def matchUni(name: String): Result = {
    val cont: List[CutMatch] = uni.trie.fuzzyMatchContItems(name, maxErrors).map(x => spell.CutMatch(x.value.split(" ").toList, x.errors))

    val fuzzy = uni.trie.fuzzyMatchItems(name, maxErrors).map(x => spell.FuzzyMatch(x.value.split(" ").toList, x.errors))

    cont ++ fuzzy
  }

  def matchPair(name: String): Result = {
    val cont: List[CutMatch] = pairs.trie.fuzzyMatchContItems(name, maxErrors).map(x => spell.CutMatch(x.value.split(" ").toList, x.errors))

    val fuzzy = pairs.trie.fuzzyMatchItems(name, maxErrors).map(x => spell.FuzzyMatch(x.value.split(" ").toList, x.errors))

    cont ++ fuzzy
  }

  def matchMulti(split: List[String]): Result = {
    val (head, last3) = split.splitAt(split.length - 3)

    val cont = triple.trie.fuzzyMatchContItems(s"${last3.head} ${last3(1)} ${last3.last}", maxErrors).map {
      res =>
        val s = res.value.split(" ").toList
        spell.CutMatch(head ++ s, res.errors)
    }

    val fuzzy = matchM(None, split, maxErrors)

    cont ++ fuzzy
  }

  def matchM(prevWord: Option[String], split: List[String], errors: Int): Result = {
    val (head, last) = split.splitAt(3)

    val fuzzy: List[MatchItem] = triple.trie.fuzzyMatchItems(head.mkString(" "), errors)

    val filter: List[MatchItem] = prevWord match {
      case Some(word) => fuzzy.filter { triples =>
        val s = triples.value.split(" ").toList
        triple.trie.exactMatch(s"$word ${s.head} ${s(1)}")
      }
      case _ => fuzzy
    }

    if (last.nonEmpty) {
      filter.flatMap { item =>
        val s = item.value.split(" ")
        val res: Result = matchM(Some(s.head), s.takeRight(2).toList ++ last, errors - item.errors.totalErrors)
        res.collect { case fuzzyMatch: FuzzyMatch =>
          spell.FuzzyMatch(List(s.head) ++ fuzzyMatch.split, item.errors)
        }
      }
    } else {
      fuzzy.map(item => spell.FuzzyMatch(item.value.split(" ").toList, item.errors))
    }
  }

  def enhanceResult(origin: String): List[(MatchResult, StatsMatch)] = {
    enhance(matchString(origin))
  }

  def enhance(res: Result): List[(MatchResult, StatsMatch)] = {
    res.map {
      case x@ExactMatch(_) => (x, enhanceStats(x.split))
      case x@FuzzyMatch(value, error) => (x, enhanceStats(value))
      case x@CutMatch(value, error) => (x, enhanceStats(value))
      case x@Split(value) => (x, enhanceStats(value))
    }
  }
}

sealed trait MatchResult {
  def split: List[String]
}

case class ExactMatch(name: String) extends MatchResult {
  val split: List[String] = name.split(" ").toList
}

case class FuzzyMatch(split: List[String], errors: ds.Error = ds.Error.empty) extends MatchResult

case class CutMatch(split: List[String], errors: ds.Error = ds.Error.empty) extends MatchResult

case class Split(split: List[String]) extends MatchResult

case class StatsMatch(uni: List[Int], pairs: List[Int], triple: List[Int]) {
  override def toString: String = s"StatsMatch(1TrieFrq${uni.mkString("[", ", ", "]")}, 2TrieFrq${pairs.mkString("[", ", ", "]")}, 3TrieFrq${triple.mkString("[", ", ", "]")})"
}

object StatsMatch {
  lazy val empty: StatsMatch = StatsMatch(List(), List(), List())
}

case class FullDataMatch(uni: TrieStat, pairs: TrieStat, triple: TrieStat, maxErrors: Int = 2)(implicit val strategy: Strategy = TrivialStrategy) extends Match with SplitJoin {
  override def toString(): String = s"FullDataMatch"
}

case class StrictDataMatch(uni: TrieStat, pairs: TrieStat, triple: TrieStat, maxErrors: Int = 2)(implicit val strategy: Strategy = TrivialStrategy) extends StrictMatch {
  override def toString(): String = s"StrictDataMatch"
}

case class ScoringMatchData(uni: CommonStat, pairs: CommonStat, triple: CommonStat) extends ScoringMatch {
  override def toString(): String = s"ScoringMatch"
}

object ScoringMatchData {
  lazy val empty: ScoringMatchData = ScoringMatchData(EmptyCommonStat, EmptyCommonStat, EmptyCommonStat)
}
