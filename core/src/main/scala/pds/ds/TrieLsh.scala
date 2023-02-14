package pds.ds

import com.clearspring.analytics.stream.membership.{BigBloomFilter, BigBloomFilterSerializer}
import pds._
import pds.ds.TrieLsh.Parameters
import pds.lsh.{MinHash, MinHashTrieStrategy}
import pds.spell.Limit

import java.io._
import scala.collection.mutable

case class LshTrieResultItem(idx: Int, finalError: Int) {}

object TrieLsh {
  case class Parameters(len: Int)

  def deserialize(prefixName: String): TrieLsh = {
    val ser = new BigBloomFilterSerializer()

    val parameters = SerializationUtils.deserialize[Parameters](new FileInputStream(new File(s"$prefixName.param.lsh")))
    val finalBloom = ser.deserialize(new DataInputStream(new FileInputStream(new File(s"$prefixName.final.lsh"))))
    val bloomFilter = BloomFilterComposite.deserialize(s"$prefixName.lsh")

    val res = TrieLsh(bloomFilter, finalBloom, parameters.len)

    res
  }

  def serialize(trie: TrieLsh, prefixName: String): Unit = {
    val ser = new BigBloomFilterSerializer()

    ser.serialize(trie.finalBloomFilter, new DataOutputStream(new FileOutputStream(s"$prefixName.final.lsh")))
    SerializationUtils.serialize(Parameters(trie.len), new FileOutputStream(new File(s"$prefixName.param.lsh")))
    BloomFilterComposite.Serializer(trie.bloomFilter).serialize(s"$prefixName.lsh")
  }

  def apply(wordsAmount: Long, stringLen: Int, avgWordLength: Int = 27): TrieLsh = {
    new TrieLsh(BloomFilterComposite.create(wordsAmount * avgWordLength.toLong, 30, 8), new BigBloomFilter(wordsAmount, 30, 8), stringLen)
  }
}

case class TrieLsh(bloomFilter: BloomFilterComposite, finalBloomFilter: BigBloomFilter, len: Int) {

  def addWord(value: String, id: Int): Unit = {
    assert(value.length == len)

    val idSuffix = MinHash.int2Str(id)
    val toAdd = value + idSuffix
    if (finalBloomFilter.put(toAdd)) {
      var i = toAdd.length
      var flag = true
      while (i >= 1 && flag) {
        val prefix = toAdd.take(i)
        if (bloomFilter.put(prefix)) {
          i = i - 1
        } else {
          flag = false
        }
      }
    }
  }

  def find(value: String, errors: Int)(implicit limit: Limit, strategy: MinHashTrieStrategy): List[LshTrieResultItem] = {
    implicit val res = mutable.Set[LshTrieResultItem]()
    findWithErrors(value, 0, "", 0, errors)(limit, res, strategy)

    res.toList
  }

  def findWithErrors(originWord: String, curLength: Int, curPrefix: String, curErrors: Int, maxErrors: Int)(implicit limit: Limit, mutableResult: mutable.Set[LshTrieResultItem], strategy: MinHashTrieStrategy): Unit = {
    if (strategy.accept(curLength, len, curErrors, maxErrors) && mutableResult.size <= limit.limit) {
      if (curErrors <= maxErrors && curLength == len) {
        findId(curPrefix, "", curErrors)
      }
      else {
        for (c <- 0 until 64) {
          val char = MinHash.numToChar(c.toChar)
          val str = curPrefix + char
          if (bloomFilter.isPresent(str)) {
            findWithErrors(originWord, curLength + 1, str, curErrors + MinHash.bitsCount(c ^ MinHash.charToNum(originWord(curLength))), maxErrors)
          }
        }
      }
    }
  }

  def findId(curPrefix: String, curNumPrefix: String, errors: Int)(implicit mutableResult: mutable.Set[LshTrieResultItem]): Unit = {
    if (finalBloomFilter.isPresent(curPrefix + curNumPrefix) && curNumPrefix.nonEmpty) {
      mutableResult.add(LshTrieResultItem(MinHash.str2Int(curNumPrefix), errors))
    }

    if (curNumPrefix.length < 10) {
      for (c <- 0 until 64) {
        val char = MinHash.numToChar(c.toChar)
        val str = curPrefix + curNumPrefix + char
        if (bloomFilter.isPresent(str)) {
          findId(curPrefix, curNumPrefix + char, errors)
        }
      }
    }
  }

  def serialize(prefixName: String): Unit = {
    import BloomFilterComposite._
    val serializer = new BigBloomFilterSerializer()
    serializer.serialize(finalBloomFilter, new DataOutputStream(new FileOutputStream(new File(s"$prefixName.final.lsh"))))
    bloomFilter.serialize(s"$prefixName.lsh")
    val parameters: Parameters = Parameters(len)
    FileUtils.write(s"$prefixName.param.lsh", SerializationUtils.serialize(parameters))
  }

  override def toString: String = s"LshTrie. Len = $len"
}
