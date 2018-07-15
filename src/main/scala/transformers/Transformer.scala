package transformers

import models.Table

class Transformer {

  def rawJsonToTable(json: String): Table = {
    val jsonTable = ujson.read(json)

    val url = jsonTable("url").str

    val title = jsonTable("title").str match {
      case s if s.isEmpty || s == null => jsonTable("pageTitle").str
      case s                           => s
    }

//    val tableType = jsonTable("tableType").str
    val tblOrientation = jsonTable("tableOrientation").str
    val relation = jsonTable("relation")

    val buff_cols = relation.arr map(_.arr.map(el => el.str).toList)
    val columns = if (tblOrientation == "HORIZONTAL") {
      buff_cols.toList
    } else {
      buff_cols.toList.transpose
    }

    val hdrRowIdx = Option(jsonTable("headerRowIndex").num.toInt).filter(_ => jsonTable("hasHeader").bool)
    val keyClnIdx = Option(jsonTable("keyColumnIndex").num.toInt).filter(_ => jsonTable("hasKeyColumn").bool)

    new Table(title, url, keyClnIdx, hdrRowIdx, columns)

//    val colsSize = columns.size
//    val rowsSize = columns.head.size

//    val headersStr = columns.map(column => column.head).mkString(" ")
//    val entitiesStr = columns.map(column => column drop 1 mkString " ").mkString(" ")
//    val keysStr = columns.head.mkString(" ")

  }

}
