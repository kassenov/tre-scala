package search

import java.io.StringReader

import models.matching.ValueMatchResult
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import similarity.{ByWordLevenshteinSimilarity, TermFrequencyScorer}
import statistics.TermFrequencyProvider

import scala.collection.mutable

trait ValueSearch {

  // TODO: returns first best match, but can be any
  def getValueMatchInValues(value: String, rowValues: List[String], exclude: List[Int]): Option[ValueMatchResult]

}

// TODO: merge with key search
class ValueSearchWithSimilarity(termFrequencyProvider: TermFrequencyProvider, analyzer: Analyzer) extends ValueSearch {

  private lazy val tfScorer = new TermFrequencyScorer(termFrequencyProvider)
  private lazy val similarity = new ByWordLevenshteinSimilarity(tfScorer)

  private val analyzedValuesCache = mutable.Map[String, String]()

  override def getValueMatchInValues(value: String, rowValues: List[String], exclude: List[Int]): Option[ValueMatchResult] = {

    val matchSim =
      rowValues
        .par
        .zipWithIndex
        .map { case (tableRow, idx) =>

          if (!exclude.contains(idx)) {
            similarity.sim(analyzeQueryValue(value), analyze(tableRow)) match {
              case Some(sim) => (idx, sim)
              case None => (idx, 0)
            }
          } else {
            (idx, 0)
          }

        }.toList
        .maxBy{ case (_, sim) => sim }

    matchSim match {
      case (idx, sim) if sim > 0 => Some(ValueMatchResult(idx, sim))
      case _ => None
    }

  }

  private def analyzeQueryValue(value: String): String = {
    if (analyzedValuesCache.contains(value)) {
      analyzedValuesCache(value)
    } else {
      val resultingString = analyze(value)

      analyzedValuesCache.put(value, resultingString)

      resultingString
    }
  }

  private def analyze(value: String): String = {
    val result = new mutable.ArrayBuffer[String]()
    val stream = analyzer.tokenStream(null, new StringReader(value))
    while (stream.incrementToken())
      result += stream.getAttribute(classOf[CharTermAttribute]).toString
    stream.end()
    stream.close()
    result.mkString(" ")
  }

}


