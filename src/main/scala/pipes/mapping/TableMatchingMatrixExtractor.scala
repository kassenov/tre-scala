package pipes.mapping

import models.matching.TableMatch
import models.matching.matrix._

class TableMatchingMatrixExtractor() {

  def extract(tableMatch: TableMatch): MatchMatrix = {

    val matchingMatrixColumns =
      tableMatch
        .keyMatches
        .map { keyMatch =>

          val matchingMatrixCells =
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

          MatchingMatrixColumn(matchingMatrixCells)

        }

    MatchMatrix(matchingMatrixColumns)

  }

}
