package pipes

import models.Table
import models.matching._
import search.{KeySearch, ValueSearch}

import scala.collection.mutable

class TableMatchingPipe(queryTable: Table, keySearch: KeySearch, valueSearch: ValueSearch) {

  def process(table: Table): TableMatching = {

    val keyValueMatches =
      getQueryKeysToTableKeyMatches(table)
        .map{ case (queryRowIdx, tableKeyMatches) =>

          val rowMatches =
            tableKeyMatches.map { keyMatch =>

              val cellMatches =
                getRowCellMatches(queryRowIdx, keyMatch, table)
                  .map { case (queryClmIdx, valueMatches) => CellMatching(valueMatches)}

              RowMatching(cellMatches)

            }

          KeyMatching(queryRowIdx, rowMatches)

        }

    TableMatching(keyValueMatches)

  }

  def getQueryKeysToTableKeyMatches(table: Table): List[(Int, List[ValueMatchResult])] = {

    val tableKeys = Table.getKeys(table)

    Table.getKeys(queryTable)
      .zipWithIndex
      .map{ case (queryKey, queryRowIdx) =>

        (keySearch.getKeyMatchInTableKeys(queryKey, tableKeys) match {
          case None         => List.empty
          case Some(keyMatch) => List(keyMatch)
        }) match {
          case list => (queryRowIdx, list)
        }

      }

  }

  def getRowCellMatches(queryRowIdx: Int, keyMatch: ValueMatchResult, table: Table): List[(Int, List[ValueMatchResult])] = {

    val tableRow = Table.getRowByIndex(keyMatch.idx, table)

    Table.getRowByIndex(queryRowIdx, queryTable)
      .zipWithIndex
      .map { case (queryRow, queryClmIdx) =>

        (valueSearch.getValueMatchInValues(queryRow, tableRow) match {
          case None         => List.empty
          case Some(valueMatch) => List(valueMatch)
        }) match {
          case list => (queryClmIdx, list)
        }

      }

  }

  private val queryTableRowsCache = mutable.Map[Int, List[String]]()

  def getQueryRow(queryRowIdx: Int): List[String] =
    queryTableRowsCache.get(queryRowIdx) match {
      case Some(row) => row
      case None =>
        val extractedRow = Table.getRowByIndex(queryRowIdx, queryTable)
        queryTableRowsCache += queryRowIdx -> extractedRow
        extractedRow
    }

}
