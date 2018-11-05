package pipes.mapping

import models.matching.TableMatch
import models.matching.matrix.{AdjacentMatches, MatchFrequencyMatrix, MatchMatrix}

class TableMatchFrequencyMatrixExtractor() {

  def extract(tableMatch: TableMatch, matchMatrix: MatchMatrix): MatchFrequencyMatrix = {

    val totalKeysFound = tableMatch.keyMatches.length
    val candTblClmnIdxToOccurencePerColumn = MatchMatrix.getCandClmnIdxToOccurrenceMapByMatrix(matchMatrix)

    val columns = matchMatrix.columns.zipWithIndex.map { case (column, queryClmnIdx) =>
      val candTblClmnIdxToOccurence = candTblClmnIdxToOccurencePerColumn(queryClmnIdx)
      column.cells.map { cell =>
        val matches = cell.idxes.distinct.map(candTblClmnIdx => candTblClmnIdxToOccurence(candTblClmnIdx))
        val adjescentPositiveMatches = matches.sum - matches.length // Removing the matches themselves
        val adjescentPossibleMatches = (totalKeysFound * matches.length) - matches.length

        AdjacentMatches(adjescentPositiveMatches, adjescentPossibleMatches)
      }
    }

    MatchFrequencyMatrix(columns)

  }

}
