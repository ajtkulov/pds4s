package pds.lsh

import pds.lsh.MinHash._

import scala.util.Random

object MinHash {
  type Hash = Int

  lazy val prime: Int = 1000000009

  /**
    * https://en.wikipedia.org/wiki/Extended_Euclidean_algorithm
    * k x == 1 (mod p)
    *
    * @param k coefficient
    * @param p prime (actually, not a prime, could be a ring, not field)
    * @return
    */
  def reverse(k: Long, p: Long): Long = {
    var t: Long = 0
    var r: Long = p
    var newt: Long = 1
    var newr: Long = k

    while (newr != 0) {
      val q = r / newr
      //      (t, newt) = (newt, t - q * newt)
      val tt = t - q * newt
      t = newt
      newt = tt
      val rr = r - q * newr
      //      (r, newr) = (newr, t - q * newr)
      r = newr
      newr = rr
    }

    assert(r <= 1)

    if (t < 0) {
      t = t + p
    }

    t
  }

  def mod(k: Long): Long = {
    val r = k % prime
    if (r < 0) {
      r + prime
    } else {
      r
    }
  }

  def hamming(fst: List[Int], snd: List[Int]): Int = {
    (fst zip snd).count(x => x._1 != x._2)
  }

  def toBinary(values: List[Int]): List[Int] = {
    values.map(_ % 2)
  }

  def int2Str(value: Int): String = {
    assert(value >= 0)

    if (value == 0) {
      "0"
    } else {
      val res = new StringBuffer()

      var r = value
      while (r > 0) {
        val q = r % 64
        r = r / 64
        res.append(numToChar(q.toChar))
      }
      res.toString.reverse
    }
  }

  def numToChar(ch: Char): Char = {
    assert(ch < 64 && ch >= 0)
    ('0' + ch).toChar
  }

  def charToNum(ch: Char): Char = {
    (ch - '0').toChar
  }

  def str2Int(value: String): Int = {
    value.reverse.map(charToNum).zip(Iterator.iterate(1)(_ * 64).take(value.length).toList).map(x => x._1 * x._2).sum
  }

  def binaryToString(values: List[Int]): String = {
    assert(values.forall(x => x == 0 || x == 1))
    val list = values.grouped(6).map(list => list.foldLeft[(Int, Int)]((0, 1)) { case ((sum, power), c) => (sum + power * c, power * 2)}._1).toList.map(x => numToChar(x.toChar))
    new String(list.toArray)
  }

  def bitsCount(value: Int): Int = {
    var i: Int = value
    i = i - ((i >> 1) & 0x55555555)
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
    (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24
  }
}

trait MinHashBasic {
  def prime: Int
  case class HashItem(k: Long, b: Long, reverse: Long)

  case class UniversalHash(prime: Int, random: Random, size: Int) {
    val items: List[HashItem] = List.range(0, size).map(_ => {
      val k = mod(random.nextInt())
      HashItem(k, mod(random.nextInt()), reverse(k, prime))
    })
  }

  def universalHash: UniversalHash

  def ngramHash(ngram: String): Int = {
    var res = 15
    for (c <- ngram) {
      res = res * 31 + c
    }

    res = res % prime
    if (res < 0) {
      res = -res
    }

    res
  }

  def minHash(ngrams: Set[String]): List[Int] = {
    val hashes: List[Hash] = ngrams.map(ngramHash).toList

    universalHash.items.map { hashItem =>
      if (hashes.isEmpty) {
        0
      } else {
        hashes.map { hash =>
          (mod((hash - hashItem.b) * hashItem.reverse), hash)
        }.minBy(_._1)._2
      }
    }
  }
}

case class MinHash(size: Int = 32) extends MinHashBasic {
  lazy val prime: Int = 1000000009
  val random: Random = new scala.util.Random(0)
  lazy val universalHash: UniversalHash = UniversalHash(prime, random, size)

  def hash[T](value: T)(implicit ripper: LshNgramRipper[T]): List[Int] = {
    minHash(ripper.cut(value))
  }

  def binaryHash[T](value: T)(implicit ripper: LshNgramRipper[T]): List[Int] = {
    toBinary(hash(value))
  }

  def stringHash[T](value: T)(implicit ripper: LshNgramRipper[T]): String = {
    binaryToString(binaryHash(value))
  }
}
