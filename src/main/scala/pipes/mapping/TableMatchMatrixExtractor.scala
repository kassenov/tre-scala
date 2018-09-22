package pipes.mapping

import models.Table
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
  def extract(queryTable: Table, tableMatch: TableMatch, tableColumnsRelations: List[TableColumnsRelation]): MatchMatrix = {

    val queryColumnsCount = queryTable.columns.length

    val rowMatchesConstraints = tableColumnsRelations.filter(r => r.linkedColumnIdxes.length > 2) // more than two is a complex relation
    val idxesInContraints = rowMatchesConstraints.flatMap(c => c.linkedColumnIdxes).toSet.filter(i => i != 0)

    val matchMatrixColumns =
      tableMatch
        .keyMatches // <- for every query key
        .map { keyMatch =>

          val rowCellsMatches = keyMatch
            .rowMatches // <- query rows
            .head // TODO Only first match
            .cellsMatches

          val matchMatrixCells =
            keyMatch
              .rowMatches // <- query rows
              .head // TODO Only first match
              .cellsMatches // <- query row's cells
              .zipWithIndex
              .map { case (queryCellMatches, queryClmnIdx) =>

                //cellMatch.queryColumnIdx

                val idxes =
                  queryCellMatches.valueMatches
                    .map (valueMatch => valueMatch.candidateColumnIdx)

                MatchingMatrixCell(idxes)

              }

          MatchingMatrixColumn(matchMatrixCells)

        }

    MatchMatrix(matchMatrixColumns)

  }



}
