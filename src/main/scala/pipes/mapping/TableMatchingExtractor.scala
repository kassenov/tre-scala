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

                val queryClmnIdxToCellMatchMap =
                  getRowCellMatches(queryTable, queryRowIdx, keyMatch.candidateIdx, candidateTable)
                    .flatMap { case (queryClmIdx, valueMatches) =>
                      if (valueMatches.isEmpty) {
                        None
                      } else {
                        Some(queryClmIdx -> CellMatch(queryClmIdx, valueMatches))
                      }
                    }.toMap

                if (queryClmnIdxToCellMatchMap.isEmpty) {
                  None
                } else {
                  Some(RowMatch(keyMatch.candidateIdx, queryClmnIdxToCellMatchMap))
                }

              }

          if (rowMatches.isEmpty || rowMatches.length > 1) { // TODO Note that duplicate keys are not allowed
            None
          } else {
            Some(QueryKeyToRowMatches(queryRowIdx, rowMatches))
          }

        }

    TableMatch(keyValueMatches, Table.getKeys(candidateTable))

  }

  private def getQueryKeyIdxToCandidateTableMatches(queryTable: Table, candidateTable: Table): List[(Int, List[ValueMatchResult])] = {

    val tableKeys = Table.getKeys(candidateTable)

    Table.getKeys(queryTable)
      .zipWithIndex
      .flatMap{ case (queryKey, queryRowIdx) =>

        if (queryKey.isDefined) {
          val matches =
            keySearch.getValueMatchesOfKeyInKeys(queryKey.get, tableKeys)
              .flatMap {
                case m if m.sim > 0 => Some(m)
                case _ => None
              }

          Some(queryRowIdx, matches)
        } else {
          None
        }

      }

  }

  private def getRowCellMatches(queryTable: Table, queryRowIdx: Int, candidateRowIdx: Int, candidateTable: Table): List[(Int, List[ValueMatchResult])] = {

    val tableRow = Table.getRowByIndex(candidateRowIdx, candidateTable)

    getQueryRow(queryTable, queryRowIdx)
      .zipWithIndex
      .map { case (queryCellValue, queryClmIdx) =>

        if (queryClmIdx == 0) { // TODO Assumption: first column is key (it might be complex?)

          (queryClmIdx, List.empty)

        } else {

          valueSearch.getValueMatchInValues(queryCellValue, tableRow, exclude = List(candidateTable.keyIdx.getOrElse(0))) match {
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
        queryTableRowsCache += queryRowIdx -> extractedRow.flatten
        extractedRow.flatten

    }

}
