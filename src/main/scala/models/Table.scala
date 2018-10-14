package models

import scala.util.Random

case class Table(docId: Int,
                 title: String,
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

    val keyColumn = table.columns(idx)
    if (table.hdrIdx.isDefined) {
      List.fill(table.hdrIdx.get + 1)(None) ++ keyColumn.slice(table.hdrIdx.get + 1, keyColumn.length)
    } else {
      keyColumn
    }

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

  def getColumnsWithRandomRows(count: Int, table: Table, shuffle: Boolean = true): List[List[Option[String]]] = {
    val idxes = List.range(0, table.columns.head.length)
    val randomIdxes = if (shuffle) {
      Random.shuffle(idxes).slice(0, count)
    } else {
      idxes.slice(0, count)
    }

    table.columns.map { c =>
      c.zipWithIndex.collect { case (x, i) if randomIdxes.contains(i) => x }
    }
  }

}
