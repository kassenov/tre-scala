package pipes

import models.mapping.ColumnsMapping
import models.matching.matrix.MatchingMatrix

class TableMappingPipe() {

  def process(matchingMatrix: MatchingMatrix): ColumnsMapping = {

    // TODO maybe don't need the check - always non empty list as matrix?
    val queryKeysCount = matchingMatrix.columns.find(c => c.cells.nonEmpty).get.cells.size

    // Filtering out by columns mismathes fraction
    val bestIdxPerColumnAfterFilteringByColumns =
      MatchingMatrix.getBestIdxPerColumn(matchingMatrix)
        .map {
          case Some(idxWithOccurrence) if idxWithOccurrence.occurrence >= queryKeysCount / 2 => Some(idxWithOccurrence)
          case _                                                                             => None
        }

//    val queryColumnsCount = matchingMatrix.columns.size
//
//    val matchesPerRowUnderThreshold =
//      List.range(0, queryKeysCount)
//        .map { queryRowIdx =>
//          MatchingMatrix
//            .getOccurrenceOfIdxesInQueryRowIdx(matchingMatrix, queryRowIdx, bestIdxPerColumnAfterFilteringByColumns)
//        }
//        .map {
//          case row if row.sum >= queryColumnsCount / 2 => List.empty
//          case row => row
//        }
//
//    val bestIdxPerColumnAfterFilteringByRows =
//      bestIdxPerColumnAfterFilteringByColumns
//        .map  {
//          case Some(idxWithOccurrence) if idxWithOccurrence.occurrence >= queryKeysCount / 2 => Some(idxWithOccurrence)
//          case _                                                 => None
//        }

  }

}
