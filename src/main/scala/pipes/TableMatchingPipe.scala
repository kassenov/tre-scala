package pipes

import models.Table
import models.matching.{CellMatching, RowMatching, TableMatching}
import search.KeySearchWithSimilarity

import scala.collection.mutable

class TableMatchingPipe(queryTable: Table, keySearch: KeySearchWithSimilarity) {

  def process(table: Table): TableMatching = {

    getQueryToTableKeysIdxMap(table)
      .map{ case (queryRowIdx, rowIdxes) =>
        rowIdxes.map(rowIdx => getRowCellMatches(queryRowIdx, rowIdx, table))
      }

  }

  def getQueryToTableKeysIdxMap(table: Table): List[(Int, List[Int])] = {

    val tableKeys = Table.getKeys(table)

    Table.getKeys(queryTable)
      .zipWithIndex
      .map{ case (queryKey, queryRowIdx) =>

        (keySearch.getRowIdxByKey(queryKey, tableKeys) match {
          case None         => List.empty
          case Some(rowIdx) => List(rowIdx)
        }) match {
          case list => (queryRowIdx, list)
        }

      }

  }

  def getRowCellMatches(queryRowIdx: Int, rowIdx: Int, table: Table): List[CellMatching] = {

  }

  val queryTableRowsCache = mutable.Map[Int, List[String]]()

  def getQueryRow(queryRowIdx: Int): List[String] =
    queryTableRowsCache.get(queryRowIdx) match {
      case Some(row) => row
      case None =>
        val extractedRow = Table.getRowByIndex(queryRowIdx, queryTable)
        queryTableRowsCache += queryRowIdx -> extractedRow
        extractedRow
    }

}
