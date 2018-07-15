package models

case class Table(title: String,
                 url: String,
                 keyIdx: Option[Int],
                 hdrIdx: Option[Int],
                 columns: List[List[String]])

object Table {

  def getKeys(table: Table): List[String] = {

    val idx = table.keyIdx match {
      case Some(keyIdx) => keyIdx
      case None         => 0
    }

    table.columns(idx)

  }

  def getRowByIndex(idx: Int, table: Table): List[String] = {
    if (rowIdxInRange(idx, table)) {
      table.columns.map(column => column(idx))
    } else {
      List.empty
    }
  }

  private def rowIdxInRange(idx: Int, table: Table): Boolean =
    idx > -1 && idx <= table.columns.head.length

}
