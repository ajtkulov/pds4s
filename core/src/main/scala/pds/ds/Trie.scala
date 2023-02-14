package pds.ds

import com.clearspring.analytics.stream.membership.{BigBloomFilter, BigBloomFilterSerializer}
import pds.ds.Trie.Parameters
import pds.spell.Limit
import pds.{FileUtils, SerializationUtils, Strategy}

import java.io._
import scala.collection.mutable

object Trie {
  case class Parameters(wordsAmount: Int, error: Double, avgWordLength: Int, cnt: Int, lengthSum: Long)

  type TransitionType = mutable.Map[String, mutable.Set[Char]]

  def deserialize(prefixName: String): Trie = {
    val ser = new BigBloomFilterSerializer()

    val parameters = SerializationUtils.deserialize[Parameters](new FileInputStream(new File(s"$prefixName.main.trie")))
    val transitions = SerializationUtils.deserialize[TransitionType](new FileInputStream(new File(s"$prefixName.tran.trie")))
    val finalBloom = ser.deserialize(new DataInputStream(new FileInputStream(new File(s"$prefixName.final.trie"))))
    val prefixBloom = ser.deserialize(new DataInputStream(new FileInputStream(new File(s"$prefixName.prefix.trie"))))

    val res = Trie(parameters.wordsAmount, parameters.error, parameters.avgWordLength, finalBloom, prefixBloom, transitions)
    res.cnt = parameters.cnt
    res.lengthSum = parameters.lengthSum

    res
  }

  def apply(wordsAmount: Int, error: Double = 1e-9, avgWordLength: Int = 24): Trie = {
    new Trie(wordsAmount, error, avgWordLength, new BigBloomFilter(wordsAmount, 30, 8), new BigBloomFilter(wordsAmount * avgWordLength, 30, 8), collection.mutable.Map[String, mutable.Set[Char]]())
  }
}

case class Trie(wordsAmount: Int, error: Double, avgWordLength: Int, finalWordsBloom: BigBloomFilter, prefixBloom: BigBloomFilter, transitions: mutable.Map[String, mutable.Set[Char]]) extends Serializable {
  var cnt: Int = 0
  var lengthSum: Long = 0
  val transitionLength = 4

  def addWord(word: String): Unit = {
    if (finalWordsBloom.put(word)) {
      cnt = cnt + 1
      lengthSum = lengthSum + word.length

      var i = word.length
      var flag = true
      while (i >= 1 && flag) {
        val prefix = word.take(i)
        if (prefixBloom.put(prefix)) {

          val last = prefix.last
          val transitionPrefix = prefix.takeRight(transitionLength + 1).dropRight(1)

          val set = if (transitions.contains(transitionPrefix)) {
            transitions(transitionPrefix)
          } else {
            mutable.Set[Char]()
          }

          set.add(last)
          transitions.put(transitionPrefix, set)
          i = i - 1
        } else {
          flag = false
        }
      }
    }
  }

  def stats: (Int, Long) = (cnt, lengthSum)

  private def getTransitions(ngram: String): mutable.Set[Char] = {
    if (transitions.contains(ngram)) {
      transitions(ngram)
    } else {
      mutable.Set[Char]()
    }
  }

  def exactMatch(word: String): Boolean = {
    finalWordsBloom.isPresent(word)
  }

  def exactMatchStrict(word: String): Boolean = {
    finalWordsBloom.isPresent(word) && word.indices.forall(idx => prefixBloom.isPresent(word.substring(0, idx + 1)))
  }

  def fuzzyMatch(originWord: String, errors: Int)(implicit limit: Limit, strategy: Strategy): List[String] = {
    implicit val res = mutable.Set[String]()
    implicit val matchResult = mutable.Set[MatchItem]()
    fuzzyMatchInternal(originWord, 0, "", 0, errors, _ + 1, Error.empty)
    res.toList
  }

  def fuzzyMatchItems(originWord: String, errors: Int)(implicit limit: Limit, strategy: Strategy): List[MatchItem] = {
    implicit val res = mutable.Set[String]()
    implicit val matchResult = mutable.Set[MatchItem]()
    fuzzyMatchInternal(originWord, 0, "", 0, errors, _ + 1, Error.empty)
    matchResult.toList
  }

  def fuzzyMatchCont(originWord: String, errors: Int)(implicit limit: Limit, strategy: Strategy): List[String] = {
    implicit val res = mutable.Set[String]()
    implicit val matchResult = mutable.Set[MatchItem]()
    fuzzyMatchInternal(originWord, 0, "", 0, errors, identity[Int], Error.empty)
    res.toList
  }

  def fuzzyMatchContItems(originWord: String, errors: Int)(implicit limit: Limit, strategy: Strategy): List[MatchItem] = {
    implicit val res = mutable.Set[String]()
    implicit val matchResult = mutable.Set[MatchItem]()
    fuzzyMatchInternal(originWord, 0, "", 0, errors, identity[Int], Error.empty)
    matchResult.toList
  }

  def fuzzyMatchInternal(originWord: String, curLength: Int, curPrefix: String, curErrors: Int, maxErrors: Int, errorFunc: Int => Int, error: Error)(implicit limit: Limit, mutableResult: mutable.Set[String], mutableMatchItems: mutable.Set[MatchItem], strategy: Strategy): Unit = {
    val errors = maxErrors - curErrors
    if (errors >= 0 && finalWordsBloom.isPresent(curPrefix) && Math.abs(originWord.length - curLength) <= errors) {
      if (!mutableResult.contains(curPrefix)) {
        mutableResult.add(curPrefix)
        mutableMatchItems.add(MatchItem(curPrefix, error.incCut(_ + Math.abs(originWord.length - curLength))))
      }
    }

    if (errors >= 0 && mutableResult.size <= limit.limit) {
      //      fuzzyMatchInternal(originWord, curLength + 1, curPrefix, curErrors + 1, maxErrors, errorFunc)  // delete
      val next: mutable.Set[Char] = getTransitions(curPrefix.takeRight(transitionLength))
      for {
        c <- next
      } {
        val str = curPrefix + c
        if (prefixBloom.isPresent(str)) {
          if (curLength < originWord.length && originWord(curLength) == c) {
            fuzzyMatchInternal(originWord, curLength + 1, str, curErrors, maxErrors, errorFunc, error.inc(c))
          } else if (curLength < originWord.length && originWord(curLength) != c) {
            if (strategy.acceptReplace(error)) {
              fuzzyMatchInternal(originWord, curLength + 1, str, curErrors + 1, maxErrors, errorFunc, error.inc(c).incReplace) // replace
            }
            if (strategy.acceptInsert(error)) {
              fuzzyMatchInternal(originWord, curLength, str, curErrors + 1, maxErrors, errorFunc, error.inc(c).incInsert) // insert
            }
          } else if (curLength >= originWord.length) {
            fuzzyMatchInternal(originWord, curLength, str, errorFunc(curErrors), maxErrors, errorFunc, error.inc(c).incCut(errorFunc))
          }
        }
      }
    }
  }

  def serialize(prefixName: String): Unit = {
    val serializer = new BigBloomFilterSerializer()
    serializer.serialize(finalWordsBloom, new DataOutputStream(new FileOutputStream(new File(s"$prefixName.final.trie"))))
    serializer.serialize(prefixBloom, new DataOutputStream(new FileOutputStream(new File(s"$prefixName.prefix.trie"))))
    FileUtils.write(s"$prefixName.tran.trie", SerializationUtils.serialize(transitions))
    val tuple: Parameters = Parameters(wordsAmount, error, avgWordLength, cnt, lengthSum)
    FileUtils.write(s"$prefixName.main.trie", SerializationUtils.serialize(tuple))
  }

  override def toString: String = stats.toString
}

case class MatchItem(value: String, errors: Error) {}

case class Error(totalReplace: Int, totalInsert: Int, cut: Int, contextWordIdx: Int, positionInWord: Int, replaceByWord: Map[Int, Int], insertByWord: Map[Int, Int]) {
  def inc(c: Char): Error = {
    if (c == ' ') {
      copy(contextWordIdx = this.contextWordIdx + 1, positionInWord = 0)
    } else {
      copy(positionInWord = this.positionInWord + 1)
    }
  }

  def incReplace: Error = {
    copy(totalReplace = this.totalReplace + 1, replaceByWord = Error.incMap(replaceByWord, contextWordIdx))
  }

  def incInsert: Error = {
    copy(totalInsert = this.totalInsert + 1, insertByWord = Error.incMap(insertByWord, contextWordIdx))
  }

  def incCut(errorFunc: Int => Int): Error = {
    copy(cut = errorFunc(this.cut))
  }

  def totalErrors: Int = totalReplace + totalInsert

  override def toString: String = s"Error(replace=$totalReplace,insert=$totalInsert,cut=$cut,contextWord=$contextWordIdx,replace=$replaceByWord,insert=$insertByWord)"
}

object Error {
  def empty: Error = Error(0, 0, 0, 0, 0, Map().withDefaultValue(0), Map().withDefaultValue(0))

  def incMap(map: scala.collection.immutable.Map[Int, Int], key: Int): scala.collection.immutable.Map[Int, Int] = {
    if (map.contains(key)) {
      map + (key -> (1 + map.apply(key)))
    } else {
      map + (key -> 1)
    }
  }
}
