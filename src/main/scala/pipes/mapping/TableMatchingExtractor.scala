package pipes.mapping

import models.Table
import models.matching._
import search.{KeySearcher, ValueSearcher}

import scala.collection.mutable

class TableMatchingExtractor(keySearch: KeySearcher, valueSearch: ValueSearcher) {

  def extract(queryTable: Table, candidateTable: Table): TableMatches = {

    val keyValueMatches =
      getQueryKeysToTableKeyMatches(queryTable, candidateTable)
        .flatMap{ case (queryRowIdx, tableKeyMatches) =>

          val rowMatches = // Row matches of the query key in the candidate table
            tableKeyMatches
              .flatMap { keyMatch =>

                val cellMatches =
                  getRowCellMatches(queryTable, queryRowIdx, keyMatch, candidateTable)
                    .flatMap { case (queryClmIdx, valueMatches) =>
                      if (valueMatches.isEmpty) {
                        None
                      } else {
                        Some(CellMatching(valueMatches))
                      }
                    }

                if (cellMatches.isEmpty) {
                  None
                } else {
                  Some(RowMatch(keyMatch.idx, cellMatches))
                }

              }

          if (rowMatches.isEmpty) {
            None
          } else {
            Some(QueryKeyToRowMatches(queryRowIdx, rowMatches))
          }

        }

    TableMatches(keyValueMatches)

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
