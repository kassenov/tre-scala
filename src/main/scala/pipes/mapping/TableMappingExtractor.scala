package pipes.mapping

import models.mapping.ColumnsMapping
import models.matching.TableMatch
import models.matching.matrix.{IdxWithScore, MatchFrequencyMatrix, MatchMatrix}
import models.score.{MappingScore, TableMappingScore}
import utls.MapScoring

class TableMappingExtractor(scoringMethod: MapScoring.Value) {

  def extract(tableMatch: TableMatch, matchMatrix: MatchMatrix, frequencyMatrix: MatchFrequencyMatrix): ColumnsMapping = {

//    if (!matchMatrix.columns.exists(c => c.cells.nonEmpty)) {
//      val a = 1
//    }

    val bestIdxPerColumn = scoringMethod match {
      case MapScoring.Simple => getBestIdxPerColumnBySimpleScoring(matchMatrix)
      case MapScoring.AdjacentMatchWeight => getBestIdxPerColumnByAdjacentMatchWeight(tableMatch, matchMatrix, frequencyMatrix)
    }

    val columns = bestIdxPerColumn.map{
      case Some(idxWithOccurrence) => Some(idxWithOccurrence.idx)
      case None => None
    }

    val columnsScore = bestIdxPerColumn.map{
      case Some(idxWithOccurrence) => Some(MappingScore(idxWithOccurrence.occurrence))
      case None => None
    }

    val aggregatedByColumns = MappingScore(score = columnsScore.flatten.map(clm => clm.score).sum)

    ColumnsMapping(
      columnIdxes = columns,
      TableMappingScore(
        columns = columnsScore,
        aggregatedByColumns = aggregatedByColumns,
        rows = List.empty,
        aggregatedByRows = MappingScore(score = 0),
        total = aggregatedByColumns
      ))

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

  def getBestIdxPerColumnBySimpleScoring(matchMatrix: MatchMatrix): List[Option[IdxWithScore]] = {
    // TODO maybe don't need the check - always non empty list as matrix?
    val queryKeysCount = matchMatrix.columns.find(c => c.cells.nonEmpty).get.cells.size

    // Filtering out by columns mismatches fraction
    MatchMatrix.getBestIdxPerColumnByCount(matchMatrix)
      .map {
        case Some(idxWithOccurrence) if idxWithOccurrence.occurrence >= queryKeysCount / 2 => Some(idxWithOccurrence)
        case _                                                                             => None
      }

  }

  def getBestIdxPerColumnByAdjacentMatchWeight(tableMatch: TableMatch,
                                               matchMatrix: MatchMatrix,
                                               frequencyMatrix: MatchFrequencyMatrix): List[Option[IdxWithScore]] = {

    // Filtering out by columns mismatches fraction
    MatchMatrix.getBestIdxPerColumnByWeight(tableMatch, matchMatrix, frequencyMatrix)
      .map {
        case Some(idxWithScore) if idxWithScore.weight > 0 => Some(idxWithScore)
        case _                                             => None
      }

  }

}
