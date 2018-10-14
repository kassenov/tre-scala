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
//  val clmnsCount = 3
//  val concept = s"countries_$clmnsCount"
  val clmnsCount = 3
  val concept = s"uefa50_$clmnsCount"
//  val clmnsCount = 2
//  val concept = s"foobal100_$clmnsCount"
//  val clmnsCount = 6
//  val concept = s"olympic2008_$clmnsCount"

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)

  var tableSearch = new LuceneTableSearcher(searcher)

  val analyzer = new StandardAnalyzer
  val algorithm = new TrexAlgorithm(reader, analyzer, concept)

  //====================================================
  val csvUtils = new CsvUtils()
  val groundTruthTable = csvUtils.importTable(name = s"truth_$concept", clmnsCount, hdrRowIdx = None)

  val queryTableColumns = Table.getColumnsWithRandomRows(count=4, groundTruthTable, shuffle = false)
  val queryTable = new Table(docId = 0,"Query", "None", keyIdx = Some(0), hdrIdx = None, columns = queryTableColumns)
//  val queryTable = csvUtils.importTable(name = s"query_$concept", clmnsCount, hdrRowIdx = Some(0))

  //====================================================

  val tableColumnsRelations = List(
//    TableColumnsRelation(List(0, 1)),
//    TableColumnsRelation(List(0, 2)),
//    TableColumnsRelation(List(0, 3)),
//    TableColumnsRelation(List(0, 4)),
//    TableColumnsRelation(List(0, 5)),
//    TableColumnsRelation(List(0, 1, 2, 3, 4, 5)),
      TableColumnsRelation(List(0, 1, 2)),
  )

  //====================================================

  println("Start")
  val startTime = System.nanoTime

  val retrievedTable = algorithm.run(queryTable, tableSearch, tableColumnsRelations)

  val endTime = System.nanoTime
  val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
  println(s"Finished indexing for $concept. Total found ${retrievedTable.columns.head.length} in $duration seconds")

  csvUtils.exportTable(queryTable, s"query_$concept")
  csvUtils.exportTable(retrievedTable, s"retrieved_$concept")

  // Searchers

  val queryTable1 = csvUtils.importTable(name = s"query_$concept", clmnsCount, hdrRowIdx = Some(0))
  val retrievedTable1 = csvUtils.importTable(name = s"retrieved_$concept", clmnsCount, hdrRowIdx = Some(0))

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
  private val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
  private val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

  val evaluator = new Evaluator(groundTruthTable, keySearcher, valueSearcher)

  val evalColumns = queryTable1.columns.zipWithIndex.map { case (clmn, clmnIdx) =>
    clmn ::: retrievedTable1.columns(clmnIdx)
  }
  val evalTable = Table(
    docId = 0,
    title = "eval",
    url = "no",
    keyIdx = Some(0),
    hdrIdx = None,
    columns = evalColumns
  )
//  val evalResults = evaluator.evaluate(retrievedTable1)
//  println(s"Evals: $evalResults")

  val a = 10

}
