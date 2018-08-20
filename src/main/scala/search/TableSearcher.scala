package search

import java.io.File

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher}
import org.apache.lucene.store.SimpleFSDirectory

import scala.collection.parallel.immutable.ParSeq

trait TableSearcher {

  var previouslyFoundDocIds: List[Int] = _

  def getRawJsonTablesByKeys(keys: List[String]): ParSeq[String]

}

class LuceneTableSearcher(indexPath: String) extends TableSearcher {

  private lazy val luceneIndexDir = new SimpleFSDirectory(new File(indexPath).toPath)

  private lazy val reader = DirectoryReader.open(luceneIndexDir)
  private lazy val searcher = new IndexSearcher(reader)

  def getRawJsonTablesByKeys(keys: List[String]): ParSeq[String] = {

    val query = buildKeysQuery(keys)
    val scoutSearchResult = searcher.search(query, 1)

    println(s"Total found ${scoutSearchResult.totalHits.toInt} tables")

    val docs = searcher
      .search(query, scoutSearchResult.totalHits.toInt)
      .scoreDocs
      .filterNot(scoreDoc => previouslyFoundDocIds.contains(scoreDoc.doc))
      .map(scoreDoc => scoreDoc.doc)
      .toList

    previouslyFoundDocIds = List(previouslyFoundDocIds, docs).flatten

    docs
      .par
      .map(doc => searcher.doc(doc).get("raw"))

  }

  private lazy val analyzer = new EnglishAnalyzer()
  private lazy val field = "keys"
  private lazy val parser = new QueryParser(field, analyzer)

  private def buildKeysQuery(keys: List[String]): BooleanQuery = {
    val builder = new BooleanQuery.Builder()

    keys.foreach { key =>
      val parsedKey = parser.parse(key)
      builder.add(parsedKey, BooleanClause.Occur.SHOULD)
    }

    val query = builder.build()

    System.out.println("Searching for: " + query.toString(field))

    query
  }

}


