package search

import java.io.StringReader

import models.Table
import models.index.IndexFields
import models.matching.ValueMatchResult
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.IndexReader
import similarity.{ByWordLevenshteinSimilarity, TermFrequencyWeighter}

import scala.collection.mutable

trait ValueSearch {

  // TODO: returns first best match, but can be any
  def getValueMatchInValues(value: String, rowValues: List[String]): Option[ValueMatchResult]

}

// TODO: merge with key search
class ValueSearchWithSimilarity(indexReader: IndexReader, analyzer: EnglishAnalyzer) extends ValueSearch {

  private lazy val tfWeighter = new TermFrequencyWeighter(indexReader, IndexFields.content)
  private lazy val similarity = new ByWordLevenshteinSimilarity(tfWeighter)

  private val analyzedValuesCache = mutable.Map[String, String]()

  override def getValueMatchInValues(value: String, rowValues: List[String]): Option[ValueMatchResult] = {

    rowValues
      .par
      .zipWithIndex
      .map { case (tableRow, idx) =>
        similarity.sim(analyzeQueryValue(value), analyze(tableRow)) match {
          case Some(sim) => (idx, sim)
        }
      }.toList
      .maxBy{ case (_, sim) => sim }

    match {
        case (idx, _) => Some(idx)
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


