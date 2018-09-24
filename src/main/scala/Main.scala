import java.io.File
import java.util.concurrent.TimeUnit

import algorithms.TrexAlgorithm
import evaluation.Evaluator
import models.Table
import models.index.IndexFields
import models.relation.TableColumnsRelation
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import search.{KeySearcherWithSimilarity, LuceneTableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import utls.CsvUtils


object Main extends App {
  val rootPath = "/media/light/SSD/50"
  val clmnsCount = 2
  val concept = s"countries_$clmnsCount"

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)

  var tableSearch = new LuceneTableSearcher(searcher)

  val analyzer = new StandardAnalyzer
  val algorithm = new TrexAlgorithm(reader, analyzer)

  //====================================================
  val csvUtils = new CsvUtils()
  val groundTruthTable = csvUtils.importTable(name = s"truth_$concept", clmnsCount, hdrRowIdx = None)

  val queryTableColumns = Table.getColumnsWithRandomRows(count=2, groundTruthTable)
  val queryTable = new Table("Query", "None", keyIdx = Some(0), hdrIdx = None, columns = queryTableColumns)

  //====================================================

  val tableColumnsRelations = List(
    TableColumnsRelation(List(0, 1)),
//    TableColumnsRelation(List(0, 2)),
//    TableColumnsRelation(List(0, 3)),
//    TableColumnsRelation(List(0, 4)),
//    TableColumnsRelation(List(0, 5)),
  )

  //====================================================

  println("Start")
  val startTime = System.nanoTime

//  val retrievedTable = algorithm.run(queryTable, tableSearch, tableColumnsRelations)

//  val endTime = System.nanoTime
//  val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
//  println(s"Finished indexing for $concept. Total found ${retrievedTable.columns.head.length} in $duration seconds")
//
//  csvUtils.exportTable(queryTable, s"query_$concept")
//  csvUtils.exportTable(retrievedTable, s"retrieved_$concept")

  // Searchers

  val retrievedTable1 = csvUtils.importTable(name = s"truth_$concept", clmnsCount, hdrRowIdx = None)
  val queryTable1 = csvUtils.importTable(name = s"truth_$concept", clmnsCount, hdrRowIdx = None)

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
  private val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
  private val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

  val evaluator = new Evaluator(groundTruthTable, keySearcher, valueSearcher)

  val evalColumns = queryTable1.columns.zipWithIndex.map { case (clmn, clmnIdx) =>
    clmn ::: retrievedTable1.columns(clmnIdx)
  }
  val evalTable = Table(
    title = "eval",
    url = "no",
    keyIdx = Some(0),
    hdrIdx = None,
    columns = evalColumns
  )
  val evalResults = evaluator.evaluate(retrievedTable1)

  val a = 10

}
