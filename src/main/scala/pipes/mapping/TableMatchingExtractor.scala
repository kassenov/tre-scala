package pipes.mapping

import models.Table
import models.matching._
import search.{KeySearcher, ValueSearcher}

import scala.collection.mutable

class TableMatchingExtractor(keySearch: KeySearcher, valueSearch: ValueSearcher) {

  def extract(queryTable: Table, candidateTable: Table): TableMatch = {

    val keyValueMatches =
      getQueryKeyIdxToCandidateTableMatches(queryTable, candidateTable)
        .flatMap{ case (queryRowIdx, tableKeyMatches) =>

          val rowMatches = // Row matches of the query key in the candidate table
            tableKeyMatches
              .flatMap { keyMatch =>

                val cellMatches =
                  getRowCellMatches(queryTable, queryRowIdx, keyMatch.candidateColumnIdx, candidateTable)
                    .flatMap { case (queryClmIdx, valueMatches) =>
                      if (valueMatches.isEmpty) {
                        None
                      } else {
                        Some(CellMatch(queryClmIdx, valueMatches))
                      }
                    }

                if (cellMatches.isEmpty) {
                  None
                } else {
                  Some(RowMatch(keyMatch.candidateColumnIdx, cellMatches))
                }

              }

          if (rowMatches.isEmpty || rowMatches.length > 1) { // TODO Note that duplicate keys are not allowed
            None
          } else {
            Some(QueryKeyToRowMatches(queryRowIdx, rowMatches))
          }

        }

    TableMatch(keyValueMatches)

  }

  private def getQueryKeyIdxToCandidateTableMatches(queryTable: Table, candidateTable: Table): List[(Int, List[ValueMatchResult])] = {

    val tableKeys = Table.getKeys(candidateTable)

    Table.getKeys(queryTable)
      .zipWithIndex
      .map{ case (queryKey, queryRowIdx) =>

        val matches =
          keySearch.getValueMatchesOfKeyInKeys(queryKey, tableKeys)
          .flatMap {
            case m if m.sim > 0 => Some(m)
            case _ => None
          }

        (queryRowIdx, matches)

      }

  }

  private def getRowCellMatches(queryTable: Table, queryRowIdx: Int, candidateRowIdx: Int, candidateTable: Table): List[(Int, List[ValueMatchResult])] = {

    val tableRow = Table.getRowByIndex(candidateRowIdx, candidateTable)

    getQueryRow(queryTable, queryRowIdx)
      .zipWithIndex
      .map { case (queryRow, queryClmIdx) =>

        if (queryClmIdx == 0) { // TODO Assumption: first column is key (it might be complex?)

          (queryClmIdx, List.empty)

        } else {

          (valueSearch.getValueMatchInValues(queryRow, tableRow, exclude = List(candidateTable.keyIdx.getOrElse(0))) match {
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
