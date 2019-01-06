package evaluation

import models.Table
import search.{KeySearcher, ValueSearcher}
import utls.Serializer

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._

case class ColumnsValuesMatchCountResult(matchValuesCount: Int,
                                         nfIdxs: List[(Int, Option[String])],
                                         nmIdxs: List[(Int, Option[String])])

case class EvaluationValuesWithNM(clmnIdx: Int,
                                  evalScore: EvaluationScore,
                                  valueMatchesCountResult: ColumnsValuesMatchCountResult)

case class NotFoundAndNotMatchedRecordsResult(nfRecs: List[List[Option[String]]],
                                              nmRecs: List[List[Option[String]]])

case class EvaluationKeysWithNM(result: EvaluationResult,
                                nfIdxs: List[(Int, Option[String])],
                                nmIdxs: List[(Int, Option[String])],
                                notFoundAndNotMatchedRecordsResult: NotFoundAndNotMatchedRecordsResult)

class Evaluator(groundTruthTable: Table,
                keySearch: KeySearcher,
                valueSearch: ValueSearcher,
                dataName: String) {

  private val serializer = new Serializer()
  lazy val clmnsCount: Int = groundTruthTable.columns.length

  def evaluate(evalTable: Table): EvaluationResult = {

    val retrievedTotalRowsCount = evalTable.columns.head.length
    val truthTotalRowsCount = groundTruthTable.columns.head.length

    val truthKeyColumn = Table.getKeys(groundTruthTable)
    val evalKeyColumn = Table.getKeys(evalTable)

    val keyTruthRowIdxToEvalRowIdx = getKeyTruthRowIdxToEvalRowIdx(truthKeyColumn, evalKeyColumn)

    val matchKeysCount = keyTruthRowIdxToEvalRowIdx.flatMap(m => m._2).toSet.size

    val keyPrecision = calculatePrecision(matchKeysCount, retrievedTotalRowsCount)
    val keyRecall = calculateRecall(matchKeysCount, truthTotalRowsCount)
    val keyScore = EvaluationScore(keyPrecision, keyRecall)

    val evalResults = List.range(1, clmnsCount).map { clmnIdx =>
      val truthColumn = groundTruthTable.columns(clmnIdx)
      val evalColumn = evalTable.columns(clmnIdx)

      val valueMatchesCountResult = calculateValuesMatchInColumns(keyTruthRowIdxToEvalRowIdx, truthColumn, evalColumn)

      val precision = calculatePrecision(valueMatchesCountResult.matchValuesCount, matchKeysCount)//retrievedTotalRowsCount)
      val recall = calculateRecall(valueMatchesCountResult.matchValuesCount, matchKeysCount) //truthTotalRowsCount)

      EvaluationValuesWithNM(clmnIdx, EvaluationScore(precision, recall), valueMatchesCountResult)

    }

    val notFoundGTKeyIdxs = keyTruthRowIdxToEvalRowIdx.filterNot(m => m._2.isDefined).keys.map(i => (i, truthKeyColumn(i))).toList
    val matchRTKeyIdxs = keyTruthRowIdxToEvalRowIdx.values.flatten.toList
    val notMatchRTKeyIdxs = List.range(0, evalKeyColumn.size).filterNot(idx => matchRTKeyIdxs.contains(idx)).map(i => (i, evalKeyColumn(i))).toList

    val notMatchedAndNotFoundRecords = getNotMatchedAndNotFoundRecords(evalResults, truthKeyColumn, evalKeyColumn)
    
    val eval = EvaluationKeysWithNM(
      EvaluationResult(columnScores = keyScore :: evalResults.map(r => r.evalScore)),
      notFoundGTKeyIdxs,
      notMatchRTKeyIdxs,
      notMatchedAndNotFoundRecords
    )

    serializer.saveAsJson(eval, s"${dataName}_EvalsData")

    eval.result

  }

  def evaluateMaxPairwise(evalTable: Table): EvaluationResult = {

    val retrievedTotalRowsCount = evalTable.columns.head.length
    val truthTotalRowsCount = groundTruthTable.columns.head.length

    val truthKeyColumn = Table.getKeys(groundTruthTable)
    val evalKeyColumn = Table.getKeys(evalTable)

    val keyTruthRowIdxToEvalRowIdx = getKeyTruthRowIdxToEvalRowIdx(truthKeyColumn, evalKeyColumn)

    val matchKeysCount = keyTruthRowIdxToEvalRowIdx.flatMap(m => m._2).toSet.size

    val keyPrecision = calculatePrecision(matchKeysCount, retrievedTotalRowsCount)
    val keyRecall = calculateRecall(matchKeysCount, truthTotalRowsCount)
    val keyScore = EvaluationScore(keyPrecision, keyRecall)

    val evalResults = List.range(1, clmnsCount).map { clmnIdx =>
      val truthColumn = groundTruthTable.columns(clmnIdx)

      // TODO: At the moment taking the max, but it might need alternation to have average and mean.
      val valueMatchesCountResult = List.range(1, evalTable.columns.length).map { subClmnIdx =>
        val evalColumn = evalTable.columns(clmnIdx)
        calculateValuesMatchInColumns(keyTruthRowIdxToEvalRowIdx, truthColumn, evalColumn)
      }.maxBy(r => r.matchValuesCount)

      val precision = calculatePrecision(valueMatchesCountResult.matchValuesCount, matchKeysCount)//retrievedTotalRowsCount)
      val recall = calculateRecall(valueMatchesCountResult.matchValuesCount, matchKeysCount) //truthTotalRowsCount)

      EvaluationValuesWithNM(clmnIdx, EvaluationScore(precision, recall), valueMatchesCountResult)

    }

    val notFoundGTKeyIdxs = keyTruthRowIdxToEvalRowIdx.filterNot(m => m._2.isDefined).keys.map(i => (i, truthKeyColumn(i))).toList
    val matchRTKeyIdxs = keyTruthRowIdxToEvalRowIdx.values.flatten.toList
    val notMatchRTKeyIdxs = List.range(0, evalKeyColumn.size).filterNot(idx => matchRTKeyIdxs.contains(idx)).map(i => (i, evalKeyColumn(i))).toList

    val notMatchedAndNotFoundRecords = getNotMatchedAndNotFoundRecords(evalResults, truthKeyColumn, evalKeyColumn)

    val eval = EvaluationKeysWithNM(
      EvaluationResult(columnScores = keyScore :: evalResults.map(r => r.evalScore)),
      notFoundGTKeyIdxs,
      notMatchRTKeyIdxs,
      notMatchedAndNotFoundRecords
    )

    serializer.saveAsJson(eval, s"${dataName}_EvalsData")

    eval.result

  }

  private def getKeyTruthRowIdxToEvalRowIdx(truthKeyColumn: List[Option[String]],
                                            evalKeyColumn: List[Option[String]]): Map[Int, Option[Int]] = {
    truthKeyColumn.zipWithIndex.par.map { case (truthKey, truthKeyIdx) =>
      if (truthKey.isDefined) {
      val valueMatch =
      //          keySearch.getValueMatchesOfKeyInKeys(truthKey.get, tableKeys.flatten)
      valueSearch.getValueMatchInValues(truthKey.get.toLowerCase(), evalKeyColumn, exclude = List.empty)
      .flatMap {
      case m if m.sim > 0.5 => Some(m)
      case _              => None
    }
      if (valueMatch.isEmpty) {
      truthKeyIdx -> None
    } else {
      truthKeyIdx -> Some(valueMatch.head.candidateColumnIdx) // <- row idx
    }
    } else {
      truthKeyIdx -> None
    }

    }.seq.toMap//.toList.sortBy(m => m._1).map(m => m._2)
  }

  private def calculateValuesMatchInColumns(keyTruthRowIdxToEvalRowIdx: Map[Int, Option[Int]],
                                            truthColumn: List[Option[String]],
                                            evalColumn: List[Option[String]]): ColumnsValuesMatchCountResult = {
    val valueTruthRowIdxToEvalRowIdx = truthColumn.zipWithIndex.par.map { case (truthValue, truthRowIdx) =>
      keyTruthRowIdxToEvalRowIdx(truthRowIdx) match {
        case Some(foundRowIdx) =>

          val foundValue = evalColumn(foundRowIdx)

          val valueMatch =
            valueSearch.getValueMatchInValues(truthValue.get.toLowerCase(), List(foundValue) , exclude = List.empty) .flatMap {
              case m if m.sim > 0 => Some(m)
              case _                => None
            }

          if (valueMatch.isDefined) { // TODO rethink as keys search
            truthRowIdx -> Some(foundRowIdx)//Some(valueMatch.get.candidateColumnIdx) // <- row idx
          } else {
            truthRowIdx -> None
          }

        case None => truthRowIdx -> None
      }

    }.seq.toMap//.toList.sortBy(m => m._1).map(m => m._2)

    val notFoundGTValueIdxs = valueTruthRowIdxToEvalRowIdx.filterNot(m => m._2.isDefined).keys.map(i => (i, truthColumn(i))).toList
    val matchRTValueIdxs = valueTruthRowIdxToEvalRowIdx.values.flatten.toList
    val notMatchRTValueIdxs = List.range(0, evalColumn.size).filterNot(idx => matchRTValueIdxs.contains(idx)).map(i => (i, truthColumn(i)))

    val matchValuesCount = valueTruthRowIdxToEvalRowIdx.flatMap(m => m._2).toSet.size//.flatten.distinct.length

    ColumnsValuesMatchCountResult(matchValuesCount, notFoundGTValueIdxs, notMatchRTValueIdxs)
  }

  private def getNotMatchedAndNotFoundRecords(evalResults: List[EvaluationValuesWithNM],
                                              truthKeyColumn: List[Option[String]],
                                              evalKeyColumn: List[Option[String]]): NotFoundAndNotMatchedRecordsResult = {
    val nfRowIdxToRecord = mutable.Map[Int, mutable.Map[Int, Option[String]]]()
    val nmRowIdxToRecord = mutable.Map[Int, mutable.Map[Int, Option[String]]]()

    evalResults.foreach { result =>
      result.valueMatchesCountResult.nfIdxs.foreach { case (rowIdx, value) =>
        if (!nfRowIdxToRecord.contains(rowIdx)) {
          nfRowIdxToRecord += rowIdx -> mutable.Map[Int, Option[String]](0 -> truthKeyColumn(rowIdx))
        }
        nfRowIdxToRecord(rowIdx) += result.clmnIdx -> value
      }

      result.valueMatchesCountResult.nmIdxs.foreach { case (rowIdx, value) =>
        if (!nmRowIdxToRecord.contains(rowIdx)) {
          nmRowIdxToRecord += rowIdx -> mutable.Map[Int, Option[String]](0 -> evalKeyColumn(rowIdx))
        }
        nmRowIdxToRecord(rowIdx) += result.clmnIdx -> value
      }
    }

    val nfRecords = nfRowIdxToRecord.map { case (rowIdx, clmnIdxToValue) =>
      List.range(0, clmnsCount).map { clmnIdx =>
        if (clmnIdxToValue.contains(clmnIdx)) {
          clmnIdxToValue(clmnIdx)
        } else {
          Some("-match-")
        }
      }
    }.toList

    val nmRecords = nmRowIdxToRecord.map { case (rowIdx, clmnIdxToValue) =>
      List.range(0, clmnsCount).map { clmnIdx =>
        if (clmnIdxToValue.contains(clmnIdx)) {
          clmnIdxToValue(clmnIdx)
        } else {
          Some("-match-")
        }
      }
    }.toList

    NotFoundAndNotMatchedRecordsResult(nfRecords, nmRecords)

  }

  private def calculatePrecision(matchCount: Int, retrievedCount: Int): Double =
    matchCount.toDouble / retrievedCount.toDouble

  private def calculateRecall(matchCount: Int, truthCount: Int): Double =
    matchCount.toDouble / truthCount.toDouble

}
