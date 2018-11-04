package pipes.mapping

import models.matching.matrix.{MatchFrequencyMatrix, MatchMatrix}

class MatchFrequencyMatrixExtractor() {

  def extract(matchMatrix: MatchMatrix): MatchFrequencyMatrix = {

    val columns = matchMatrix.columns.map { column =>
      column.cells.map(cell => cell.idxes.distinct.length)
    }

    val total = matchMatrix.columns.flatten { column =>
      column.cells.flatten(cell => cell.idxes).toSet
    }.toSet.size

    MatchFrequencyMatrix(nPossible = total, columns)

  }

}
