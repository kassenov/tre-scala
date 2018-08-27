package pipes.mapping

import models.matching.TableMatching
import models.matching.matrix._

class TableMatchingMatrixExtractor() {

  def extract(tableMatching: TableMatching): MatchMatrix = {

    val matchingMatrixColumns =
      tableMatching
        .keyMatches
        .map { keyMatch =>

          // TODO Only first match
          val matchingMatrixCells =
            keyMatch.rowMatchings.head.cellMatches
              .zipWithIndex
              .map { case (cellMatch, queryClmIdx) =>

                val idxes =
                  cellMatch.valueMatches
                    .map (valueMatch => valueMatch.idx)

                MatchingMatrixCell(idxes)

              }

          MatchingMatrixColumn(matchingMatrixCells)

        }

    MatchMatrix(matchingMatrixColumns)

  }

}
