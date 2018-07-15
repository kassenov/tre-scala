import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher}
import org.apache.lucene.queryparser.classic.QueryParser
import search.{LuceneTableSearch, TableSearch}


object Main extends App {
  val rootPath = "/media/light/SSD/50"
  val concept = "countries"

  var tableSearch = new LuceneTableSearch(s"$rootPath/lucene-indexes/$concept-lidx")

  println("Start")
  val startTime = System.nanoTime



  val endTime = System.nanoTime
  val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
  println(s"Finished indexing for $concept. Total found ${initialSearchResult.totalHits}, indexed $totalIndexed in $duration seconds")

  val a = 10

}
