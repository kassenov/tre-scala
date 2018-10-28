package search

import java.io.File

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher}
import org.apache.lucene.store.SimpleFSDirectory

import scala.collection.parallel.immutable.{ParMap, ParSeq}

trait TableSearcher {

  var previouslyFoundDocIds: List[Int] = List.empty

  def getRelevantDocIdsByKeys(keys: List[Option[String]]): List[Int]

  def getHitsByKeys(keys: List[Option[String]]): Int

  def getRawJsonTablesByDocIds(docIds: List[Int]): ParMap[Int, String]

  def getRawJsonTableByDocId(docId: Int): String

  def getRawJsonTablesByKeys(keys: List[Option[String]]): ParSeq[String]

  def buildKeysQuery(keys: List[String]): BooleanQuery

}

class LuceneTableSearcher(indexSearcher: IndexSearcher) extends TableSearcher {

//  private val luceneIndexDir = new SimpleFSDirectory(new File(indexPath).toPath)
//
//  private val reader = DirectoryReader.open(luceneIndexDir)
//  private val searcher = new IndexSearcher(reader)

  def getRelevantDocIdsByKeys(keys: List[Option[String]]): List[Int] = {

    val query = buildKeysQuery(keys.flatten)
    val scoutSearchResult = indexSearcher.search(query, 1)

    println(s"Total found ${scoutSearchResult.totalHits.toInt} tables")

    val docs = indexSearcher
      .search(query, scoutSearchResult.totalHits.toInt)
      .scoreDocs
      .filterNot(scoreDoc => previouslyFoundDocIds.contains(scoreDoc.doc))
      .map(scoreDoc => scoreDoc.doc)
      .toList

    List(previouslyFoundDocIds, docs).flatten

  }

  def getHitsByKeys(keys: List[Option[String]]): Int = {

    if (keys.flatten.nonEmpty) {
      try {
        val query = buildKeysQuery(keys.flatten)
        indexSearcher.search(query, 1).totalHits.toInt
      } catch {
        case _: Throwable =>
          println(s"Error for key $keys")
          0
      }
    } else {
      0
    }

  }

  def getRawJsonTablesByDocIds(docIds: List[Int]): ParMap[Int, String] = {
    docIds
      .par
      .map(doc => (doc, indexSearcher.doc(doc).get("raw")))
      .toMap
  }

  def getRawJsonTableByDocId(docId: Int): String =
    indexSearcher.doc(docId).get("raw")

  def getRawJsonTablesByKeys(keys: List[Option[String]]): ParSeq[String] = {

    val query = buildKeysQuery(keys.flatten)
    val scoutSearchResult = indexSearcher.search(query, 1)

    println(s"Total found ${scoutSearchResult.totalHits.toInt} tables")

    val docs = indexSearcher
      .search(query, scoutSearchResult.totalHits.toInt)
      .scoreDocs
      .filterNot(scoreDoc => previouslyFoundDocIds.contains(scoreDoc.doc))
      .map(scoreDoc => scoreDoc.doc)
      .toList

    previouslyFoundDocIds = List(previouslyFoundDocIds, docs).flatten

    docs
      .par
      .map(doc => indexSearcher.doc(doc).get("raw"))

  }

  private lazy val analyzer = new EnglishAnalyzer()
  private lazy val field = "keys"
  private lazy val parser = new QueryParser(field, analyzer)

  def buildKeysQuery(keys: List[String]): BooleanQuery = {
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


