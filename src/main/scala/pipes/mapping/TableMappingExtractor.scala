package pipes.mapping

import models.mapping.ColumnsMapping
import models.matching.matrix.MatchMatrix
import models.score.{MappingScore, TableMappingScore}

class TableMappingExtractor() {

  def process(matchMatrix: MatchMatrix): ColumnsMapping = {

    // TODO maybe don't need the check - always non empty list as matrix?
    val queryKeysCount = matchMatrix.columns.find(c => c.cells.nonEmpty).get.cells.size

    // Filtering out by columns mismatches fraction
    val bestIdxPerColumnAfterFilteringByColumns =
      MatchMatrix.getBestIdxPerColumn(matchMatrix)
        .map {
          case Some(idxWithOccurrence) if idxWithOccurrence.occurrence >= queryKeysCount / 2 => Some(idxWithOccurrence)
          case _                                                                             => None
        }

    val columns = bestIdxPerColumnAfterFilteringByColumns.map{
      case Some(idxWithOccurrence) => Some(idxWithOccurrence.idx)
      case None => None
    }

    val columnsScore = bestIdxPerColumnAfterFilteringByColumns.map{
      case Some(idxWithOccurrence) => Some(MappingScore(idxWithOccurrence.occurrence))
      case None => None
    }

    ColumnsMapping(columns = columns, TableMappingScore(columns = columnsScore, rows = List.empty))

//    val queryColumnsCount = matchMatrix.columns.size
//
//    val matchesPerRowUnderThreshold =
//      List.range(0, queryKeysCount)
//        .map { queryRowIdx =>
//          MatchMatrix
//            .getOccurrenceOfIdxesInQueryRowIdx(matchMatrix, queryRowIdx, bestIdxPerColumnAfterFilteringByColumns)
//        }
//        .map {
//          case row if row.sum >= queryColumnsCount / 2 => List.empty
//          case row                                     => row
//        }
//
//    val bestIdxPerColumnAfterFilteringByRows =
//      bestIdxPerColumnAfterFilteringByColumns
//        .map  {
//          case Some(idxWithOccurrence) if idxWithOccurrence.occurrence >= queryKeysCount / 2 => Some(idxWithOccurrence)
//          case _                                                                             => None
//        }

  }

}
