package pipes

import models.mapping.ColumnsMapping
import models.matching.TableMatching

class TableMatchingMatrixExtractingPipe() {

  def process(tableMatching: TableMatching): ColumnsMapping = {

    tableMatching
      .keyMatches
      .map { keyMatch =>
        // TODO Only first match
        keyMatch.rowMatchings.head.cellMatches
          .zipWithIndex
          .map { case (cellMatch, queryClmIdx) =>
            // TODO Only first match
            cellMatch.valueMatches.head.idx
          }
      }

  }

}
