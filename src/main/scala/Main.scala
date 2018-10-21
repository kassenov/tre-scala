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
import utls.{CsvUtils, TaskFlow}
import transformers.Transformer

import scala.io.Source


object Main extends App {

  val transformer = new Transformer()

  val configsStr = Source.fromFile("config.json").getLines.mkString
  val configs = transformer.rawJsonToAppConfig(configsStr)

  val rootPath = "/media/light/SSD/50"
  val concept = s"${configs.concept}_${configs.columnsCount}"
//  val clmnsCount = 3
//  val concept = s"countries_$clmnsCount"
//  val clmnsCount = 3
//  val concept = s"uefa50_$clmnsCount"
//  val clmnsCount = 2
//  val concept = s"foobal100_$clmnsCount"
//  val clmnsCount = 6
//  val concept = s"olympic2008_$clmnsCount"

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)
  var tableSearch = new LuceneTableSearcher(searcher)
  val analyzer = new StandardAnalyzer

  //====================================================
  val csvUtils = new CsvUtils()
  val groundTruthTable = csvUtils.importTable(name = s"truth_$concept", configs.columnsCount, hdrRowIdx = Some(0))

  //====================================================

  configs.task match {
    case TaskFlow.Mapping =>

      val queryTableColumns = Table.getColumnsWithRandomRows(count=4, groundTruthTable, shuffle = false)
      val queryTable = new Table(docId = 0,"Query", "None", keyIdx = Some(0), hdrIdx = Some(0), columns = queryTableColumns)

      val tableColumnsRelations = List(
        //    TableColumnsRelation(List(0, 1)),
        //    TableColumnsRelation(List(0, 2)),
        //    TableColumnsRelation(List(0, 3)),
        //    TableColumnsRelation(List(0, 4)),
        //    TableColumnsRelation(List(0, 5)),
        //    TableColumnsRelation(List(0, 1, 2, 3, 4, 5)),
        TableColumnsRelation(List(0, 1, 2)),
      )

      println("Start")
      val startTime = System.nanoTime

      val algorithm = new TrexAlgorithm(reader, tableSearch, analyzer, concept, tableColumnsRelations)
      val retrievedTable = algorithm.run(queryTable)

      val endTime = System.nanoTime
      val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
      println(s"Finished indexing for $concept. Total found ${retrievedTable.columns.head.length} in $duration seconds")

      csvUtils.exportTable(queryTable, s"query_$concept")
      csvUtils.exportTable(retrievedTable, s"retrieved_$concept")

    case TaskFlow.Evaluating =>

      val queryTable1 = csvUtils.importTable(name = s"query_$concept", configs.columnsCount, hdrRowIdx = Some(0))
      val retrievedTable1 = csvUtils.importTable(name = s"retrieved_$concept", configs.columnsCount, hdrRowIdx = Some(0))

      val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
      val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

      val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
      val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

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
//      val evalResults = evaluator.evaluate(retrievedTable1)
      val evalResults = evaluator.evaluate(evalTable)
      println(s"Evals: $evalResults")

    case TaskFlow.KeysAnalysis =>

      val b = 12

  }

  val a = 10

}
