package search

import java.io.StringReader

import models.matching.ValueMatchResult
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import similarity.{ByWordLevenshteinSimilarity, TermFrequencyScorer}
import statistics.TermFrequencyProvider

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._

trait ValueSearcher {

  // TODO: returns first best match, but can be any
  def getValueMatchInValues(value: String, rowValues: List[Option[String]], exclude: List[Int], threshold: Double = 0.8): List[ValueMatchResult]

}

// TODO: merge with key search
class ValueSearcherWithSimilarity(termFrequencyProvider: TermFrequencyProvider, analyzer: Analyzer) extends ValueSearcher {

  private lazy val tfScorer = new TermFrequencyScorer(termFrequencyProvider)
  private lazy val similarity = new ByWordLevenshteinSimilarity(tfScorer)

  private val analyzedValuesCache = mutable.Map[String, String]()

  override def getValueMatchInValues(value: String, rowValues: List[Option[String]], exclude: List[Int], threshold: Double = 0.8): List[ValueMatchResult] = {

    val matchSims =
      rowValues
        .par
        .zipWithIndex
        .map { case (cellValue, idx) =>

          if (!exclude.contains(idx) && cellValue.isDefined) {
            similarity.sim(analyzeQueryValue(value), analyze(cellValue.get)) match {
              case sim => (idx, sim)
            }
          } else {
            (idx, 0.0)
          }

        }.toList
//        .maxBy{ case (_, sim) => sim }

    matchSims flatMap {
      case (idx, sim) if sim > threshold => Some(ValueMatchResult(idx, sim))
      case _ => None
    }

  }

  private def analyzeQueryValue(value: String): String = {
    if (analyzedValuesCache.contains(value)) {
      try {
        analyzedValuesCache(value)
      } catch {
        case _: Throwable => putToCache(value)
      }
    } else {
      putToCache(value)
    }
  }

  private def putToCache(value: String): String = {
    val resultingString = analyze(value)

    analyzedValuesCache.put(value, resultingString)

    resultingString
  }

  private def analyze(value: String): String = {
    val result = new mutable.ArrayBuffer[String]()
    val stream = analyzer.tokenStream(null, new StringReader(value))
    stream.reset()
    while (stream.incrementToken())
      result += stream.getAttribute(classOf[CharTermAttribute]).toString
    stream.end()
    stream.close()
    result.mkString(" ")
  }

}


