package pipes.mapping

import models.matching.TableMatching
import models.matching.matrix._

class TableMatchingMatrixExtractor() {

  def process(tableMatching: TableMatching): MatchMatrix = {

    MatchMatrix apply
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
