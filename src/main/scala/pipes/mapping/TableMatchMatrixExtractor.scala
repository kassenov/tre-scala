package pipes.mapping

import models.matching.TableMatch
import models.matching.matrix._
import models.relation.TableColumnsRelation

class TableMatchMatrixExtractor() {

  /**
    * Extracts table match matrix from table match
    *
    * @param tableMatch table keys and values matches
    * @param tableColumnsRelations relation between columns
    * @return
    */
  def extract(tableMatch: TableMatch, tableColumnsRelations: List[TableColumnsRelation]): MatchMatrix = {

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
