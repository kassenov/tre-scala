import java.io.File
import java.util.concurrent.TimeUnit

import Main.reader
import algorithms.TrexAlgorithm
import models.Table
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher}
import org.apache.lucene.queryparser.classic.QueryParser
import search.{LuceneTableSearcher, TableSearcher}


object Main extends App {
  val rootPath = "/media/light/SSD/50"
  val concept = "countries"

  var tableSearch = new LuceneTableSearcher(s"$rootPath/lucene-indexes/$concept-lidx")

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)
  val analyzer = new StandardAnalyzer
  val algorithm = new TrexAlgorithm(reader, analyzer)

  println("Start")
  val startTime = System.nanoTime

  val queryTableColumns = List(List("Russia"), List("Moscow"))
  val queryTable = new Table("Query", "None", keyIdx = Some(0), hdrIdx = None, columns = queryTableColumns)
  val result = algorithm.run(queryTable, tableSearch)

  val endTime = System.nanoTime
  val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
  println(s"Finished indexing for $concept. Total found ${result.length} in $duration seconds")

  val a = 10

}
