package models.matching.matrix


case class MatchingMatrixColumn(cells: List[MatchingMatrixCell])

object MatchingMatrixColumn {

  def getCandClmnIdxToOccurrenceMap(column: MatchingMatrixColumn): Map[Int, Int] =
    column.cells
      .flatMap(cell => cell.idxes)
      .groupBy(identity)
      .mapValues(_.size)

  def getCandClmnIdxToQueryRowIdxsMap(column: MatchingMatrixColumn): Map[Int, List[Int]] =
    column.cells
      .flatMap(cell => cell.idxes)
      .groupBy(identity)
      .mapValues(_.toList)

  def isRowHasIdx(column: MatchingMatrixColumn, queryRowIdx: Int, idx: Int): Boolean =
    column
      .cells(queryRowIdx)
      .idxes.contains(idx)

}


