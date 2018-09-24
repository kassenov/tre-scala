package evaluation

import models.Table
import search.{KeySearcher, ValueSearcher}

class Evaluator(groundTruthTable: Table, keySearch: KeySearcher, valueSearch: ValueSearcher) {

  lazy val clmnsCount: Int = groundTruthTable.columns.length

  def evaluate(evalTable: Table): EvaluationResult = {

    val retrievedTotalRowsCount = evalTable.columns.length
    val truthTotalRowsCount = groundTruthTable.columns.length

    val truthRowIdxToEvalRowIdx =
      Table.getKeys(groundTruthTable).map { truthKey =>
        val tableKeys = Table.getKeys(evalTable)
        val matches =
          keySearch.getValueMatchesOfKeyInKeys(truthKey, tableKeys)
            .flatMap {
              case m if m.sim > 0 => Some(m)
              case _              => None
            }

        if (matches.isEmpty) {
          None
        } else {
          Some(matches.head.candidateColumnIdx) // <- row idx
        }
      }

    val matchKeysCount = truthRowIdxToEvalRowIdx.flatten.length

    val keyPrecision = calculatePrecision(matchKeysCount, retrievedTotalRowsCount)
    val keyRecall = calculateRecall(matchKeysCount, truthTotalRowsCount)
    val keyScore = EvaluationScore(keyPrecision, keyRecall)

    val scores = List.range(1, clmnsCount).map { clmnIdx =>
      groundTruthTable.columns(clmnIdx).map { truthValue =>
        val tableClmnValues = evalTable.columns(clmnIdx)
        val matches =
          valueSearch.getValueMatchInValues(truthValue, tableClmnValues, exclude = List.empty) .flatMap {
            case m if m.sim > 0 => Some(m)
            case _              => None
          }

        if (matches.isEmpty) {
          None
        } else {
          Some(matches.head.candidateColumnIdx) // <- row idx
        }

      }

      val matchesCount = truthRowIdxToEvalRowIdx.flatten.length

      val precision = calculatePrecision(matchesCount, retrievedTotalRowsCount)
      val recall = calculateRecall(matchesCount, truthTotalRowsCount)
      EvaluationScore(precision, recall)

    }

    EvaluationResult(columnScores = keyScore :: scores)

  }

  private def calculatePrecision(matchCount: Int, retrievedCount: Int): Double =
    matchCount / retrievedCount

  private def calculateRecall(matchCount: Int, truthCount: Int): Double =
    matchCount / truthCount

}
