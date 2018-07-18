package models.matching.matrix

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
