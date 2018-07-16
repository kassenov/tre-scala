package pipes

import models.matching.TableMatching
import models.matching.matrix._

class TableMatchingMatrixExtractingPipe() {

  def process(tableMatching: TableMatching): MatchingMatrix = {

    MatchingMatrix apply
      tableMatching
        .keyMatches
        .map { keyMatch =>

          // TODO Only first match
          MatchingMatrixColumn apply
            keyMatch.rowMatchings.head.cellMatches
              .zipWithIndex
              .map { case (cellMatch, queryClmIdx) =>

                MatchingMatrixCell apply
                  cellMatch.valueMatches
                    .map (valueMatch => valueMatch.idx)

              }

        }

  }

}
