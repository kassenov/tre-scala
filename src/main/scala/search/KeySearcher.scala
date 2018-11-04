package search

import java.io.StringReader

import models.matching.ValueMatchResult
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import similarity.{ByWordLevenshteinSimilarity, TermFrequencyScorer}
import statistics.TermFrequencyProvider

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._

trait KeySearcher {

  def getValueMatchesOfKeyInKeys(key: String, tableKeys: List[Option[String]]): List[ValueMatchResult]

}

class KeySearcherWithSimilarity(termFrequencyProvider: TermFrequencyProvider, analyzer: Analyzer) extends KeySearcher {

  private lazy val tfScorer = new TermFrequencyScorer(termFrequencyProvider)
  private lazy val similarity = new ByWordLevenshteinSimilarity(tfScorer)

  private val analyzedKeysCache = mutable.Map[String, String]()

  override def getValueMatchesOfKeyInKeys(key: String, tableKeys: List[Option[String]]): List[ValueMatchResult] = {

    tableKeys
      .par
      .zipWithIndex
      .map { case (tableKey, idx) =>

        if (tableKey.isDefined) {
          similarity.sim(analyzeQueryKey(key), analyze(tableKey.get)) match {
            case sim => ValueMatchResult(idx, sim)
          }
        } else {
          ValueMatchResult(idx, 0.0)
        }

      }.toList

  }

  private def analyzeQueryKey(key: String): String = {
    if (analyzedKeysCache.contains(key)) {
      analyzedKeysCache(key)
    } else {
      val resultingString = analyze(key)

      analyzedKeysCache.put(key, resultingString)

      resultingString
    }
  }

  private def analyze(key: String): String = {
    val result = new mutable.ArrayBuffer[String]()
    val stream = analyzer.tokenStream(null, new StringReader(key))
    stream.reset()
    while (stream.incrementToken())
      result += stream.getAttribute(classOf[CharTermAttribute]).toString
    stream.end()
    stream.close()
    result.mkString(" ")
  }

}


