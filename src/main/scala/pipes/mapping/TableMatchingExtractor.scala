package pipes.mapping

import models.Table
import models.matching._
import search.{KeySearcher, ValueSearcher}

import scala.collection.mutable

class TableMatchingExtractor(keySearch: KeySearcher, valueSearch: ValueSearcher) {

  def extract(queryTable: Table, table: Table): TableMatching = {

    val keyValueMatches =
      getQueryKeysToTableKeyMatches(queryTable, table)
        .map{ case (queryRowIdx, tableKeyMatches) =>

          val rowMatches =
            tableKeyMatches.map { keyMatch =>

              val cellMatches =
                getRowCellMatches(queryTable, queryRowIdx, keyMatch, table)
                  .map { case (queryClmIdx, valueMatches) => CellMatching(valueMatches)}

              RowMatching(keyMatch.idx, cellMatches)

            }

          KeyMatching(queryRowIdx, rowMatches)

        }

    TableMatching(keyValueMatches)

  }

  private def getQueryKeysToTableKeyMatches(queryTable: Table, table: Table): List[(Int, List[ValueMatchResult])] = {

    val tableKeys = Table.getKeys(table)

    Table.getKeys(queryTable)
      .zipWithIndex
      .map{ case (queryKey, queryRowIdx) =>

        val matches = keySearch.getValueMatchesOfKeyInKeys(queryKey, tableKeys)
        (queryRowIdx, matches)

      }

  }

  private def getRowCellMatches(queryTable: Table, queryRowIdx: Int, keyMatch: ValueMatchResult, table: Table): List[(Int, List[ValueMatchResult])] = {

    val tableRow = Table.getRowByIndex(keyMatch.idx, table)

    getQueryRow(queryTable, queryRowIdx)
      .zipWithIndex
      .map { case (queryRow, queryClmIdx) =>

        if (queryClmIdx == 0) {

          (queryClmIdx, List.empty)

        } else {

          (valueSearch.getValueMatchInValues(queryRow, tableRow, exclude = List(table.keyIdx.getOrElse(0))) match {
            case None             => List.empty
            case Some(valueMatch) => List(valueMatch)
          }) match {
            case list => (queryClmIdx, list)
          }

        }

      }

  }

  private val queryTableRowsCache = mutable.Map[Int, List[String]]()

  private def getQueryRow(queryTable: Table, queryRowIdx: Int): List[String] =
    queryTableRowsCache.get(queryRowIdx) match {
      case Some(row) => row
      case None =>

        val extractedRow = Table.getRowByIndex(queryRowIdx, queryTable)
        queryTableRowsCache += queryRowIdx -> extractedRow
        extractedRow

    }

}
