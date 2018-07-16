package models.matching.matrix

case class MatchingMatrix (columns: List[MatchingMatrixColumn])

object MatchingMatrix {

  def getBestIdxPerColumn(matrix: MatchingMatrix): List[Option[IdxWithOccurrence]] =
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


  def getIdxToOccurrenceMapByMatrix(matrix: MatchingMatrix): List[Map[Int, Int]] =
    matrix.columns
      .map { column =>
        MatchingMatrixColumn.getIdxToOccurrenceMap(column)
      }

  def getOccurrenceOfIdxesInQueryRowIdx(matrix: MatchingMatrix,
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
