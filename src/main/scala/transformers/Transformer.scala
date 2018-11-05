package transformers

import models.Table
import utls.{AppConfig, MapScoring, TaskFlow}

class Transformer {

  def rawJsonToTable(docId: Int, json: String): Table = {
    val jsonTable = ujson.read(json)

    val url = jsonTable("url").str

    val title = jsonTable("title").str match {
      case s if s.isEmpty || s == null => jsonTable("pageTitle").str
      case s                           => s
    }

//    val tableType = jsonTable("tableType").str
    val tblOrientation = jsonTable("tableOrientation").str
    val relation = jsonTable("relation")

    val buff_cols = relation.arr map(_.arr.map(el => Some(el.str)).toList)
    val columns = if (tblOrientation == "HORIZONTAL") {
      buff_cols.toList
    } else {
      buff_cols.toList.transpose
    }

    val hdrRowIdx = Option(jsonTable("headerRowIndex").num.toInt).filter(_ => jsonTable("hasHeader").bool)
    val keyClnIdx = Option(jsonTable("keyColumnIndex").num.toInt).filter(_ => jsonTable("hasKeyColumn").bool)

    new Table(docId, title, url, keyClnIdx, hdrRowIdx, columns)

//    val colsSize = columns.size
//    val rowsSize = columns.head.size

//    val headersStr = columns.map(column => column.head).mkString(" ")
//    val entitiesStr = columns.map(column => column drop 1 mkString " ").mkString(" ")
//    val keysStr = columns.head.mkString(" ")

  }

  def rawJsonToAppConfig(json: String): AppConfig = {

    val jsonConfig = ujson.read(json)

    val queryRowsCount = jsonConfig("query_rows_count").num.toInt
    val concept = jsonConfig("concept").str
    val clmnsCount = jsonConfig("clmns_count").num.toInt
    val task = TaskFlow.withName(jsonConfig("task").str)
    val scoringMethod = MapScoring.withName(jsonConfig("scoring").str)

    val buffCols = jsonConfig("clmns_relations").arr map(_.arr.map(el => el.num.toInt).toList)
    val docIds = jsonConfig("doc_ids").arr.map(el => el.num.toInt).toList

    AppConfig(task, queryRowsCount, clmnsCount, buffCols.toList, concept, docIds, scoringMethod)

  }

}
