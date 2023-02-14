package pds.lsh

import pds.lsh.LshNgramRipper._

object LshNgramRipper {
  def mult(values: Set[String], upTo: Int): Set[String] = {
    val list = values.toList.sorted

    if (list.isEmpty) {
      Set()
    } else {
      val res = for {
        idx <- 0 to (upTo - 1) / list.size
        ng <- list
      } yield {
        s"$idx$ng"
      }

      res.take(upTo).toSet
    }
  }

  def extend(values: (Set[String], Int)*): Set[String] = {
    values.flatMap(x => mult(x._1, x._2)).toSet
  }
}

trait LshNgramRipper[T] {
  def cut(value: T): Set[String]
}

object AddressOnlyCompanyRipper extends LshNgramRipper[String] {
  override def cut(value: String): Set[String] = {
    extend(
      DefaultStringRipper.cut(value) -> 100
    )
  }
}

case class SimpleStringRipper(size: Int) extends LshNgramRipper[String] {
  def cut(value: String): Set[String] = {
    value.sliding(size).toSet
  }
}

object DefaultStringRipper extends SimpleStringRipper(3) {}
