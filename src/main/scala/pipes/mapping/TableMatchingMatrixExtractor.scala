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
