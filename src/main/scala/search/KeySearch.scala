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

trait KeySearch {

  // TODO: returns first best match, but can be any
  def getKeyMatchInTableKeys(key: String, tableKeys: List[String]): Option[ValueMatchResult]

}

class KeySearchWithSimilarity(indexReader: IndexReader, analyzer: EnglishAnalyzer) extends KeySearch {

  private lazy val tfWeighter = new TermFrequencyWeighter(indexReader, IndexFields.attributes)
  private lazy val similarity = new ByWordLevenshteinSimilarity(tfWeighter)

  private val analyzedKeysCache = mutable.Map[String, String]()

  override def getKeyMatchInTableKeys(key: String, tableKeys: List[String]): Option[ValueMatchResult] = {

    tableKeys
      .par
      .zipWithIndex
      .map { case (tableKey, idx) =>
        similarity.sim(analyzeQueryKey(key), analyze(tableKey)) match {
          case Some(sim) => (idx, sim)
        }
      }.toList
      .maxBy{ case (_, sim) => sim }

    match {
        case (idx, _) => Some(idx)
    }

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
    while (stream.incrementToken())
      result += stream.getAttribute(classOf[CharTermAttribute]).toString
    stream.end()
    stream.close()
    result.mkString(" ")
  }

}


