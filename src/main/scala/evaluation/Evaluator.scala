package evaluation

import models.Table
import search.{KeySearcher, ValueSearcher}

class Evaluator(groundTruthTable: Table, keySearch: KeySearcher, valueSearch: ValueSearcher) {

  lazy val clmnsCount: Int = groundTruthTable.columns.length

  def evaluate(evalTable: Table): EvaluationResult = {

    val retrievedTotalRowsCount = evalTable.columns.head.length
    val truthTotalRowsCount = groundTruthTable.columns.head.length

    val evalTableKeys = Table.getKeys(evalTable)

    val truthRowIdxToEvalRowIdx =
      Table.getKeys(groundTruthTable).zipWithIndex.par.map { case (truthKey, truthKeyIdx) =>

        val matches =
//          keySearch.getValueMatchesOfKeyInKeys(truthKey.get, tableKeys.flatten)
          valueSearch.getValueMatchInValues(truthKey.get.toLowerCase(), evalTableKeys, exclude = List.empty)
            .flatMap {
              case m if m.sim > 0 => Some(m)
              case _              => None
            }

        if (matches.isEmpty) {
          truthKeyIdx -> None
        } else {
          truthKeyIdx -> Some(matches.head.candidateColumnIdx) // <- row idx
        }
      }.toList.sortBy(m => m._1).map(m => m._2)

    val matchKeysCount = truthRowIdxToEvalRowIdx.flatten.length

    val keyPrecision = calculatePrecision(matchKeysCount, retrievedTotalRowsCount)
    val keyRecall = calculateRecall(matchKeysCount, truthTotalRowsCount)
    val keyScore = EvaluationScore(keyPrecision, keyRecall)

    val scores = List.range(1, clmnsCount).map { clmnIdx =>
      val truthClmnColumn = groundTruthTable.columns(clmnIdx)
      val tableClmnColumn = evalTable.columns(clmnIdx)
      truthClmnColumn.zipWithIndex.map { case (truthValue, truthRowIdx) =>
        truthRowIdxToEvalRowIdx(truthRowIdx) match {
          case Some(foundRowIdx) =>

            val foundValue = tableClmnColumn(foundRowIdx)

            val valueMatch =
              valueSearch.getValueMatchInValues(truthValue.get.toLowerCase(), List(foundValue) , exclude = List.empty) .flatMap {
                case m if m.sim > 0 => Some(m)
                case _              => None
              }

            if (valueMatch.isDefined) {
              Some(valueMatch.get.candidateColumnIdx) // <- row idx
            } else {
              None
            }

          case None => None
        }

      }

      val precision = calculatePrecision(matchKeysCount, retrievedTotalRowsCount)
      val recall = calculateRecall(matchKeysCount, truthTotalRowsCount)
      EvaluationScore(precision, recall)

    }

    EvaluationResult(columnScores = keyScore :: scores)

  }

  private def calculatePrecision(matchCount: Int, retrievedCount: Int): Double =
    matchCount.toDouble / retrievedCount.toDouble

  private def calculateRecall(matchCount: Int, truthCount: Int): Double =
    matchCount.toDouble / truthCount.toDouble

}
