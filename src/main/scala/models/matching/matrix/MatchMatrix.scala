package models.matching.matrix

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

  def getBestIdxPerColumn(matrix: MatchMatrix): List[Option[IdxWithOccurrence]] =
    getIdxToOccurrenceMapByMatrix(matrix)
      .map { columnIdxToOccurrenceMap =>

        if (columnIdxToOccurrenceMap.nonEmpty) {
          columnIdxToOccurrenceMap.max match {
            case (idx, occurrence) => Some(IdxWithOccurrence(idx, occurrence))
          }
        } else {
          None
        }
      }


  def getIdxToOccurrenceMapByMatrix(matrix: MatchMatrix): List[Map[Int, Int]] =
    matrix.columns
      .map { column =>
        MatchingMatrixColumn.getIdxToOccurrenceMap(column)
      }

  def getOccurrenceOfIdxesInQueryRowIdx(matrix: MatchMatrix,
                                        queryRowIdx: Int,
                                        bestIdxPerColumn: List[Option[IdxWithOccurrence]]): List[Int] =
    matrix.columns
      .zipWithIndex
      .map { case (column, queryClmIdx) =>
        bestIdxPerColumn(queryClmIdx) match {
          case Some(idxWithOccurrence)
            if MatchingMatrixColumn.isRowHasIdx(column, queryRowIdx, idxWithOccurrence.idx) => 1
          case _                                                                            => 0
        }
      }

}
