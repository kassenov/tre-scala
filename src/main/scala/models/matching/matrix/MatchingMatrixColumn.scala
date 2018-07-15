package models.matching.matrix


case class MatchingMatrixColumn(cells: List[MatchingMatrixCell])

object MatchingMatrixColumn {

  def getIdxToOccurrenceMap(column: MatchingMatrixColumn): Map[Int, Int] =
    column.cells
      .flatMap(cell => cell.idxes)
      .groupBy(identity).mapValues(_.size)

}


