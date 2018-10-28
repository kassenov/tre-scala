package evaluation

import java.io.StringReader

import models.Table
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.{IndexReader, Term}
import org.apache.lucene.search.IndexSearcher
import search.{LuceneTableSearcher, TableSearcher}
import utls.CsvUtils

import scala.collection.mutable

class KeysAnalysis(concept: String, indexSearcher: IndexSearcher, analyzer: Analyzer) {

  val csvUtils = new CsvUtils()
  val tableSearcher = new LuceneTableSearcher(indexSearcher)

  def generate(table: Table): Unit = {

    val records = Table.getKeys(table).par.map { key =>
//      val term = analyze(key)
//      val frequency = indexReader.totalTermFreq(new Term("keys", term))
      val totalFound = tableSearcher.getHitsByKeys(List(key))
      key :: List(Some(totalFound.toString))
    }.toList

    val columns = List.range(0, 2).map { clmIdx =>
      records.map(r => r(clmIdx))
    }

    val tableKeys = Table(
      docId = -1,
      title = "keys",
      url = "no",
      keyIdx = Some(0),
      hdrIdx = None,
      columns = columns
    )

    csvUtils.exportTable(tableKeys, s"${concept}_keys")

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
