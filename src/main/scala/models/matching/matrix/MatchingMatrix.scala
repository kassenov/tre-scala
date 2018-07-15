package models.matching.matrix

case class MatchingMatrix (columns: List[MatchingMatrixColumn])

object MatchingMatrix {

  def getBestIdxPerColumn(matrix: MatchingMatrix): List[Option[Int]] =
    getIdxToOccurrenceMapByMatrix(matrix)
      .map { columnIdxToOccurrenceMap =>

        if (columnIdxToOccurrenceMap.nonEmpty) {
          columnIdxToOccurrenceMap.max match {
            case (idx, _) => Some(idx)
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

}
