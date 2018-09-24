package models

import scala.util.Random

case class Table(title: String,
                 url: String,
                 keyIdx: Option[Int],
                 hdrIdx: Option[Int],
                 columns: List[List[Option[String]]])

object Table {

  def getKeys(table: Table): List[Option[String]] = {

    val idx = table.keyIdx match {
      case Some(keyIdx) => keyIdx
      case None         => 0
    }

    table.columns(idx)

  }

  def getRowByIndex(idx: Int, table: Table): List[Option[String]] = {
    if (rowIdxInRange(idx, table)) {
      table.columns.map(column => column(idx))
    } else {
      List.empty
    }
  }

  private def rowIdxInRange(idx: Int, table: Table): Boolean =
    idx > -1 && idx <= table.columns.head.length

  def getColumnsWithRandomRows(count: Int, table: Table): List[List[Option[String]]] = {
    val idxes = List.range(0, table.columns.head.length)
    val randomIdxes = Random.shuffle(idxes).slice(0, count)

    table.columns.map { c =>
      c.zipWithIndex.collect { case (x, i) if randomIdxes.contains(i) => x }
    }
  }

}
