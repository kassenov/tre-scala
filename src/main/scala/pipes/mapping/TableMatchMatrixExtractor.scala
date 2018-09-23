package pipes.mapping

import models.Table
import models.matching.TableMatch
import models.matching.matrix._
import models.relation.TableColumnsRelation

import scala.collection.mutable.ListBuffer

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

    val matchMtrxClmnsWithIdxes = List.fill(queryColumnsCount) {ListBuffer[List[Int]]()}

    tableMatch
      .keyMatches // <- for every query key
      .foreach { keyMatch =>

      val rowCellsMatches = keyMatch
        .rowMatches // <- query rows
        .head // TODO Only first match
        .cellsMatches

      tableColumnsRelations.foreach { relation =>
        val relatedMatchCellsOfRow = relation.linkedColumnIdxes.map { queryClmIdx =>

          val candidateColumnIdxes =
            rowCellsMatches(queryClmIdx).valueMatches.map(valueMatch => valueMatch.candidateColumnIdx)

          (queryClmIdx, candidateColumnIdxes)

        }

        // Relations constraint
        if (!relatedMatchCellsOfRow.exists { case (_, candidateColumnIdxes) => candidateColumnIdxes.isEmpty }) {

          relatedMatchCellsOfRow.foreach{ case (queryClmIdx, candidateColumnIdxes) =>
            matchMtrxClmnsWithIdxes(queryClmIdx) += candidateColumnIdxes
          }

        } else {

          // TODO log?

        }

      }

    }

    val matchMatrixColumns = matchMtrxClmnsWithIdxes.map { listOfIdxes =>
      MatchingMatrixColumn(listOfIdxes.toList.map(idxes => MatchingMatrixCell(idxes)))
    }

    MatchMatrix(matchMatrixColumns)

  }

}
