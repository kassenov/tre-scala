package pipes.mapping

import models.matching.TableMatch
import models.matching.matrix._

class TableMatchMatrixExtractor() {

  def extract(tableMatch: TableMatch): MatchMatrix = {

    val matchMatrixColumns =
      tableMatch
        .keyMatches
        .map { keyMatch =>

          val matchMatrixCells =
            keyMatch
              .rowMatches
              .head // TODO Only first match
              .cellMatches
              .map { cellMatch =>

                val idxes =
                  cellMatch.valueMatches
                    .map (valueMatch => valueMatch.candidateColumnIdx)

                MatchingMatrixCell(idxes)

              }

          MatchingMatrixColumn(matchMatrixCells)

        }

    MatchMatrix(matchMatrixColumns)

  }

}
