package pipes

import models.matching.TableMatching
import models.matching.matrix._

class TableMatchingMatrixExtractingPipe() {

  def process(tableMatching: TableMatching): MatchingMatrix = {

    val columns =
      tableMatching
        .keyMatches
        .map { keyMatch =>
          // TODO Only first match
          val cells =
            keyMatch.rowMatchings.head.cellMatches
              .zipWithIndex
              .map { case (cellMatch, queryClmIdx) =>
                val idxes =
                  cellMatch.valueMatches
                    .map (valueMatch => valueMatch.idx)

                MatchingMatrixCell(idxes)
              }

          MatchingMatrixColumn(cells)
        }

    MatchingMatrix(columns)

  }

}
