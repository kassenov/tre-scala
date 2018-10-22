package evaluation

import models.Table
import search.{KeySearcher, ValueSearcher}
import utls.Serializer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class EvaluationValuesWithNM(clmnIdx: Int,
                                  evalScore: EvaluationScore,
                                  nfIdxs: List[(Int, Option[String])],
                                  nmIdxs: List[(Int, Option[String])])

case class EvaluationKeysWithNM(result: EvaluationResult,
                                nfIdxs: List[(Int, Option[String])],
                                nmIdxs: List[(Int, Option[String])],
                                values: List[EvaluationValuesWithNM],
                                nfRecs: List[List[Option[String]]],
                                nmRecs: List[List[Option[String]]])

class Evaluator(groundTruthTable: Table,
                keySearch: KeySearcher,
                valueSearch: ValueSearcher,
                dataName: String) {

  private val serializer = new Serializer()
  lazy val clmnsCount: Int = groundTruthTable.columns.length

  def evaluate(evalTable: Table): EvaluationResult = {

    val retrievedTotalRowsCount = evalTable.columns.head.length
    val truthTotalRowsCount = groundTruthTable.columns.head.length

    val groundTruthKeys = Table.getKeys(groundTruthTable)
    val evalTableKeys = Table.getKeys(evalTable)

    val keyTruthRowIdxToEvalRowIdx =
      groundTruthKeys.zipWithIndex.par.map { case (truthKey, truthKeyIdx) =>

        if (truthKey.isDefined) {
          val valueMatch =
          //          keySearch.getValueMatchesOfKeyInKeys(truthKey.get, tableKeys.flatten)
            valueSearch.getValueMatchInValues(truthKey.get.toLowerCase(), evalTableKeys, exclude = List.empty)
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

    val matchKeysCount = keyTruthRowIdxToEvalRowIdx.flatMap(m => m._2).toSet.size

    val keyPrecision = calculatePrecision(matchKeysCount, retrievedTotalRowsCount)
    val keyRecall = calculateRecall(matchKeysCount, truthTotalRowsCount)
    val keyScore = EvaluationScore(keyPrecision, keyRecall)

    val results = List.range(1, clmnsCount).map { clmnIdx =>
      val truthClmnColumn = groundTruthTable.columns(clmnIdx)
      val tableClmnColumn = evalTable.columns(clmnIdx)
      val valueTruthRowIdxToEvalRowIdx = truthClmnColumn.zipWithIndex.par.map { case (truthValue, truthRowIdx) =>
        keyTruthRowIdxToEvalRowIdx(truthRowIdx) match {
          case Some(foundRowIdx) =>

            val foundValue = tableClmnColumn(foundRowIdx)

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

      val notFoundGTValueIdxs = valueTruthRowIdxToEvalRowIdx.filterNot(m => m._2.isDefined).keys.map(i => (i, truthClmnColumn(i))).toList
      val matchRTValueIdxs = valueTruthRowIdxToEvalRowIdx.values.flatten.toList
      val notMatchRTValueIdxs = List.range(0, tableClmnColumn.size).filterNot(idx => matchRTValueIdxs.contains(idx)).map(i => (i, tableClmnColumn(i)))

      val matchValuesCount = valueTruthRowIdxToEvalRowIdx.flatMap(m => m._2).toSet.size//.flatten.distinct.length

      val precision = calculatePrecision(matchValuesCount, matchKeysCount)//retrievedTotalRowsCount)
      val recall = calculateRecall(matchValuesCount, matchKeysCount) //truthTotalRowsCount)

      EvaluationValuesWithNM(clmnIdx, EvaluationScore(precision, recall), notFoundGTValueIdxs, notMatchRTValueIdxs)

    }

    val notFoundGTKeyIdxs = keyTruthRowIdxToEvalRowIdx.filterNot(m => m._2.isDefined).keys.map(i => (i, groundTruthKeys(i))).toList
    val matchRTKeyIdxs = keyTruthRowIdxToEvalRowIdx.values.flatten.toList
    val notMatchRTKeyIdxs = List.range(0, evalTableKeys.size).filterNot(idx => matchRTKeyIdxs.contains(idx)).map(i => (i, evalTableKeys(i))).toList

    val nfRowIdxToRecord = mutable.Map[Int, mutable.Map[Int, Option[String]]]()
    val nmRowIdxToRecord = mutable.Map[Int, mutable.Map[Int, Option[String]]]()

    results.foreach { result =>
      List.range(1, clmnsCount).foreach { clmnIdx =>
        result.nfIdxs.foreach { case (rowIdx, value) =>
          if (!nfRowIdxToRecord.contains(rowIdx)) {
            nfRowIdxToRecord += rowIdx -> mutable.Map[Int, Option[String]](0 -> groundTruthKeys(rowIdx))
          }
          nfRowIdxToRecord(rowIdx) += clmnIdx -> value
        }

        result.nmIdxs.foreach { case (rowIdx, value) =>
          if (!nmRowIdxToRecord.contains(rowIdx)) {
            nmRowIdxToRecord += rowIdx -> mutable.Map[Int, Option[String]](0 -> evalTableKeys(rowIdx))
          }
          nmRowIdxToRecord(rowIdx) += clmnIdx -> value
        }
      }
    }

    val nfRecords = nfRowIdxToRecord.map { case (rowIdx, clmnIdxToValue) =>
      List.range(0, clmnsCount).map { clmnIdx =>
        if (clmnIdxToValue.contains(clmnIdx)) {
          clmnIdxToValue(clmnIdx)
        } else {
          None
        }
      }
    }.toList

    val nmRecords = nmRowIdxToRecord.map { case (rowIdx, clmnIdxToValue) =>
      List.range(0, clmnsCount).map { clmnIdx =>
        if (clmnIdxToValue.contains(clmnIdx)) {
          clmnIdxToValue(clmnIdx)
        } else {
          None
        }
      }
    }.toList

    val eval = EvaluationKeysWithNM(
      EvaluationResult(columnScores = keyScore :: results.map(r => r.evalScore)),
      notFoundGTKeyIdxs,
      notMatchRTKeyIdxs,
      results,
      nfRecords,
      nmRecords
    )

    serializer.saveAsJson(eval, s"${dataName}_EvalsData")

    eval.result

  }

  private def calculatePrecision(matchCount: Int, retrievedCount: Int): Double =
    matchCount.toDouble / retrievedCount.toDouble

  private def calculateRecall(matchCount: Int, truthCount: Int): Double =
    matchCount.toDouble / truthCount.toDouble

}
