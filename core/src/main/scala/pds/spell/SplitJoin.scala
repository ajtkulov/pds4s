package pds.spell

import Match._

trait SplitJoin extends Match {
  def splitJoin2(name: String): Result = {
    //maxErrors + 1, +1 means extra space
    pairs.trie.fuzzyMatch(name, maxErrors + 1).map(x => Split(x.split(" ").toList))
  }

  def splitJoin3(name: String): Result = {
    triple.trie.fuzzyMatch(name, maxErrors + 2).map(x => Split(x.split(" ").toList))
  }

  def splitJoin1Dumb(name: String): Result = {
    if (uni.trie.exactMatchStrict(name)) {
      List(Split(List(name)))
    } else {
      List()
    }
  }

  def splitJoin2Dumb(name: String): Result = {
    val res = for {
      i <- 1 until name.length
      fst = name.substring(0, i)
      snd = name.substring(i)
      str = fst + " " + snd
      if pairs.trie.exactMatch(str)
    } yield {
      Split(List(fst, snd))
    }

    res.toList
  }

  def splitJoin2Dumbest(name: String): Result = {
    val res = for {
      i <- 1 until name.length
      fst = name.substring(0, i)
      snd = name.substring(i)
      if (uni.trie.exactMatchStrict(fst) && uni.trie.exactMatchStrict(snd))
    } yield {
      Split(List(fst, snd))
    }

    res.toList
  }

  def recSplit(name: String, step: Int, accum: List[String], maxStep: Int = 8): Iterator[List[String]] = {
    if (step >= maxStep) {
      Iterator.empty
    } else {
      (name.length to 1 by -1).filter(size => uni.trie.exactMatch(name.substring(0, size))).toIterator.flatMap {
        size =>
          if (size == name.size) {
            Iterator.single(accum :+ name)
          } else {
            recSplit(name.substring(size), step + 1, accum :+ name.substring(0, size), maxStep)
          }
      }
    }
  }

  def smartSplit(name: String): Result = {
    val fst = (1 to 6).find { maxStep => recSplit(name, 0, List(), maxStep).nonEmpty }
    fst.map(size => recSplit(name, 0, List(), size + 1).toList.map(x => Split(x))).getOrElse(List())
  }

  def enhanceSplitResult(origin: String): List[(MatchResult, StatsMatch)] = {
    enhance(smartSplit(origin))
  }
}
