package models.matching.matrix

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class MatchingMatrixColumn(cells: List[MatchingMatrixCell])

object MatchingMatrixColumn {

  def getCandClmnIdxToOccurrenceMap(column: MatchingMatrixColumn): Map[Int, Int] =
    column.cells
      .flatMap(cell => cell.idxes)
      .groupBy(identity)
      .mapValues(_.size)

  def getCandClmnIdxToQueryRowIdxsMap(column: MatchingMatrixColumn): Map[Int, List[Int]] = {
    val candClmnIdxToQueryRowIdxsMap = mutable.Map[Int, ListBuffer[Int]]()
    column.cells.zipWithIndex.foreach { case (cell, queryRowIdx) =>
      cell.idxes.foreach { candClmnIdx =>
        if (!candClmnIdxToQueryRowIdxsMap.contains(candClmnIdx)) {
          candClmnIdxToQueryRowIdxsMap(candClmnIdx) = ListBuffer.empty
        }
        candClmnIdxToQueryRowIdxsMap(candClmnIdx) += queryRowIdx
      }
    }

    candClmnIdxToQueryRowIdxsMap.map { case (clmnIdx, queryRowIdxs) =>
        clmnIdx -> queryRowIdxs.toList
    }.toMap
  }

  def isRowHasIdx(column: MatchingMatrixColumn, queryRowIdx: Int, idx: Int): Boolean =
    column
      .cells(queryRowIdx)
      .idxes.contains(idx)

}


