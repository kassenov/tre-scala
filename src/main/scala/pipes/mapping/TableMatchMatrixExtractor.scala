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
  def extract(queryColumnsCount: Int, queryKeysCount: Int, tableMatch: TableMatch, tableColumnsRelations: List[TableColumnsRelation]): MatchMatrix = {

    val matchMtrxClmnsWithIdxes = List.fill(queryColumnsCount) {ListBuffer.fill(queryKeysCount)(List[Int]())}

    tableMatch
      .keyMatches // <- for every query key
      .foreach { keyMatch =>

      val queryClmIdxCellMatchMap = keyMatch
        .rowMatches // <- query rows
        .head // TODO Only first match
        .queryClmIdxCellMatchMap

      tableColumnsRelations.foreach { relation =>
        val relatedMatchCellsOfRow = relation.linkedColumnIdxes.filter(i => i > 0).flatMap { queryClmIdx =>

          if (queryClmIdxCellMatchMap.contains(queryClmIdx)) {
            val candidateColumnIdxes =
              queryClmIdxCellMatchMap(queryClmIdx).valueMatches.map(valueMatch => valueMatch.candidateIdx)

            Some(queryClmIdx, candidateColumnIdxes)
          } else {
            None
          }

        }

        // Relations constraint
        if (!relatedMatchCellsOfRow.exists { case (_, candidateColumnIdxes) => candidateColumnIdxes.isEmpty }) {

          relatedMatchCellsOfRow.foreach{ case (queryClmIdx, candidateColumnIdxes) =>
            matchMtrxClmnsWithIdxes(queryClmIdx)(keyMatch.queryRowIdx) = candidateColumnIdxes
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
