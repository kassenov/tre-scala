package models.matching.matrix

import models.matching.TableMatch

/**
  * Match Matrix is a data structure that keeps matches of original table in another table using indexes
  * and simplifies extracting a mapping between those tables.
  *
  * Match matrix from original table (ot - query table) to destination table (dt):
  *
  *               ot.clmn-1   |   ot.clmn-2   | *** |   ot.clmn-n
  * ot.rec-1 -> [dt.clmn-idx] | [dt.clmn-idx] | *** | [dt.clmn-idx]
  * ot.rec-2 -> [dt.clmn-idx] | [dt.clmn-idx] | *** | [dt.clmn-idx]
  *   ***            ***      |      ***      | *** |      ***
  * ot.rec-m -> [dt.clmn-idx] | [dt.clmn-idx] | *** | [dt.clmn-idx]
  *
  * Each cell in the match matrix keeps set of column indexes where query key match key and query value match value.
  *
  * @param columns
  */
case class MatchMatrix(columns: List[MatchingMatrixColumn])

object MatchMatrix {

  def getBestIdxPerColumnByCount(matrix: MatchMatrix): List[Option[IdxWithScore]] =
    getCandClmnIdxToOccurrenceMapByMatrix(matrix)
      .map { candClmnIdxToOccurrenceMap =>

        if (candClmnIdxToOccurrenceMap.nonEmpty) {
          candClmnIdxToOccurrenceMap.maxBy{
            case (_, occurrence) => occurrence
          } match {
            case (idx, occurrence) => Some(IdxWithScore(idx, occurrence, occurrence))
          }
        } else {
          None
        }
      }

  def getCandClmnIdxToOccurrenceMapByMatrix(matrix: MatchMatrix): List[Map[Int, Int]] =
    matrix.columns
      .map { column =>
        MatchingMatrixColumn.getCandClmnIdxToOccurrenceMap(column)
      }

  def getOccurrenceOfIdxesInQueryRowIdx(matrix: MatchMatrix,
                                        queryRowIdx: Int,
                                        bestIdxPerColumn: List[Option[IdxWithScore]]): List[Int] =
    matrix.columns
      .zipWithIndex
      .map { case (column, queryClmIdx) =>
        bestIdxPerColumn(queryClmIdx) match {
          case Some(idxWithOccurrence)
            if MatchingMatrixColumn.isRowHasIdx(column, queryRowIdx, idxWithOccurrence.idx) => 1
          case _                                                                            => 0
        }
      }

  def getBestIdxPerColumnByWeight(tableMatch: TableMatch,
                                  matchMatrix: MatchMatrix,
                                  frequencyMatrix: MatchFrequencyMatrix): List[Option[IdxWithScore]] = {

    val candTblClmnIdxToOccurencePerColumn = MatchMatrix.getCandClmnIdxToOccurrenceMapByMatrix(matchMatrix)

    matchMatrix.columns.zipWithIndex
      .map { case(mtrxColumn, queryClmnIdx) =>
        val candClmnIdxToQueryRowIdxs = MatchingMatrixColumn.getCandClmnIdxToQueryRowIdxsMap(mtrxColumn)

        if (candClmnIdxToQueryRowIdxs.nonEmpty && candClmnIdxToQueryRowIdxs.exists(m => m._2.nonEmpty)) {

          val frequencyColumn = frequencyMatrix.columns(queryClmnIdx)
          val totalPossibleWeight = tableMatch.keyMatches.map { keyMatch =>
            AdjacentMatches.getWeight(frequencyColumn(keyMatch.queryRowIdx))
          }.sum

          candClmnIdxToQueryRowIdxs.map { case (candClmnIdx, queryRowIdxs) =>
              val weight = queryRowIdxs.map { queryRowIdx =>
                AdjacentMatches.getWeight(frequencyColumn(queryRowIdx))
              }.sum
              candClmnIdx -> (weight, queryRowIdxs.length)
          }.maxBy {
            case (_, (weight, _)) => weight
          } match {
            case (idx, (weight, occurrence)) => Some(IdxWithScore(idx, occurrence, weight))
          }

        } else {
          None
        }

      }
  }

}
