import java.io.File
import java.util.concurrent.TimeUnit

import Main.getRandomTable
import algorithms.TrexAlgorithm
import evaluation.query.QueryTableEvaluator
import evaluation.result.{EvaluationResult, Evaluator, KeysAnalysis}
import models.Table
import models.index.IndexFields
import models.relation.TableColumnsRelation
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import search.{KeySearcherWithSimilarity, LuceneTableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import utls.{CsvUtils, Serializer, TaskFlow}
import transformers.Transformer

import scala.io.Source


object Main extends App {

  private val serializer = new Serializer()
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

//  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/6mln-keys-to-row-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)
  var tableSearch = new LuceneTableSearcher(searcher)
  val analyzer = new StandardAnalyzer

  //====================================================
  val csvUtils = new CsvUtils()
  val groundTruthTable = csvUtils.importTableByName(name = s"truth_$concept", configs.columnsCount, hdrRowIdx = Some(0))

  //====================================================

  val docsNum = reader.numDocs()
  println(docsNum)

  configs.task match {
    case TaskFlow.Mapping =>

      val (queryTable, retrievedTable) = doMapping(s"$concept${configs.queryRowsCount}", groundTruthTable, configs.queryRowsCount, shuffle = false)

      csvUtils.exportTable(queryTable, s"query_$concept${configs.queryRowsCount}")
      csvUtils.exportTable(retrievedTable, s"retrieved_$concept${configs.queryRowsCount}")

    case TaskFlow.QueryTableEval =>

      val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
      val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

      val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
      val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

      val queryTableColumns = Table.getColumnsWithRandomRows(count=configs.queryRowsCount, groundTruthTable, shuffle = false)
      val queryTable = new Table(docId = 0,"Query", "None", keyIdx = Some(0), hdrIdx = Some(0), columns = queryTableColumns)

      val tableColumnsRelations = configs.columnsRelations.map(rel => TableColumnsRelation(rel))

      println("Start")
      val startTime = System.nanoTime

      val algorithm = new QueryTableEvaluator(reader, tableSearch, analyzer, s"$concept${configs.queryRowsCount}", tableColumnsRelations, configs.scoringMethod, configs.maxK, keySearcher, valueSearcher, None)
      val retrievedTable = algorithm.run(queryTable, true)

      val endTime = System.nanoTime
      val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
      println(s"Finished indexing for $concept in $duration seconds")

    case TaskFlow.KeyValueEntropyEval =>

      val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
      val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

      val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
      val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

      val groundTruthKeys = Table.getKeys(groundTruthTable)
      val results = List.range(1, groundTruthTable.columns.head.length) map { targetRowIdx =>
        println(s"----------------------  $targetRowIdx  ---------------------------------------")
        val oneRowTableColumns = groundTruthTable.columns.map { c =>
          c.zipWithIndex.collect { case (x, i) if i == 0 || i == targetRowIdx => x }
        }

        val queryTable = new Table(docId = 0,"Query", "None", keyIdx = Some(0), hdrIdx = Some(0), columns = oneRowTableColumns)
        val tableColumnsRelations = configs.columnsRelations.map(rel => TableColumnsRelation(rel))

        println("Start")
        val startTime = System.nanoTime

        val algorithm = new TrexAlgorithm(reader, tableSearch, analyzer, s"${concept}_kve_$targetRowIdx", tableColumnsRelations, configs.scoringMethod, configs.maxK)
        val retrievedTable = algorithm.run(queryTable)

        val endTime = System.nanoTime
        val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
        println(s"Finished indexing for $concept. Total found ${retrievedTable.columns.head.length} in $duration seconds")

        csvUtils.exportTable(queryTable, s"query_$concept${configs.queryRowsCount}")
        csvUtils.exportTable(retrievedTable, s"retrieved_$concept${configs.queryRowsCount}")

        //-------------------

        println("Start")
        val startTime2 = System.nanoTime

        val algorithm2 = new QueryTableEvaluator(reader, tableSearch, analyzer, s"${concept}_kve_$targetRowIdx", tableColumnsRelations, configs.scoringMethod, configs.maxK, keySearcher, valueSearcher, Some(groundTruthKeys))
        val result = algorithm2.run(queryTable, true)

        val endTime2 = System.nanoTime
        val duration2 = TimeUnit.NANOSECONDS.toSeconds(endTime2 - startTime2)
        println(s"Finished evel for $concept in $duration2 seconds")

        println(s"---------------------------------------------------------------")

        result

      }

      val columns = List.range(0, groundTruthTable.columns.length * 2 - 1).map { clmIdx =>
        results.map { r =>
          clmIdx match {
            case 0 =>
              r.queryKeys(1)
            case n if n < groundTruthTable.columns.length =>
              if (r.clmnIdxToNToCount(clmIdx).contains(1)) {
                Some(r.clmnIdxToNToCount(clmIdx)(1).toString)
              } else {
                None
              }
            case _ =>
              if (r.clmnIdxToNToEntropy.isDefined) {
                if (r.clmnIdxToNToEntropy.get(clmIdx - groundTruthTable.columns.length + 1).contains(1)) {
                  Some(r.clmnIdxToNToEntropy.get(clmIdx - groundTruthTable.columns.length + 1)(1).toString)
                } else {
                  None
                }
              } else {
                None
              }
          }
        }
      }

      val countsTable = Table(
        docId = -1,
        title = "retrieved",
        url = "no",
        keyIdx = Some(0),
        hdrIdx = None,
        columns = columns
      )

      csvUtils.exportTable(countsTable, s"counts_$concept${configs.queryRowsCount}")

    case TaskFlow.Evaluating =>

      val queryTable1 = csvUtils.importTableByName(name = s"query_$concept${configs.queryRowsCount}", configs.columnsCount, hdrRowIdx = Some(0))
      val retrievedTable1 = csvUtils.importTableByName(name = s"retrieved_$concept${configs.queryRowsCount}", configs.columnsCount, hdrRowIdx = Some(0))

      val evalResults = doEval(s"$concept${configs.queryRowsCount}", queryTable1, retrievedTable1)
      println(s"Evals: $evalResults")

    case TaskFlow.LargeExperiment =>
//      get100Tables()
      doMassiveExperimentMapping()
//      doMassiveExperimentEval()

    case TaskFlow.ExtEvalPairWise =>

      val retrievedTablePath = configs.extParams.head
      val retrievedTable = csvUtils.importTableByPath(retrievedTablePath, hdrRowIdx = Some(0))

      val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
      val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

      val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
      val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

      val evaluator = new Evaluator(groundTruthTable, keySearcher, valueSearcher, s"$concept${configs.queryRowsCount}")

      val evalResults = evaluator.evaluateMaxPairwise(retrievedTable)
      println(s"ExtEvals: $evalResults")

    case TaskFlow.KeysAnalysis =>

      val keysAnalyzer = new KeysAnalysis(concept, searcher, analyzer)
      keysAnalyzer.generate(groundTruthTable)

    case TaskFlow.Extracting =>

      tableSearch.getRawJsonTablesByDocIds(configs.docIds).par
        .map { case (docId, jsonTable) => transformer.rawJsonToTable(docId, jsonTable) }
        .foreach { candidateTable =>
          serializer.saveAsJson(candidateTable, candidateTable.docId.toString,"extracted")
        }

  }

  def get100Tables() = {
    val tables = List.range(1, 100).map { i =>
      getRandomTable()
    }
    serializer.serialize(tables, "100_tables")
  }

  def doMassiveExperimentMapping() = {
    val tables = serializer.deserialize("100_tables").asInstanceOf[List[Table]]

    val result = tables.map { randomTable =>
      // save table as ground truth csv
      val truthName = s"truth_${randomTable.docId}_1"
      csvUtils.exportTable(randomTable, truthName)

      // generate query random 10%
      val rowsCount = groundTruthTable.columns.head.length
      val (queryTable, retrievedTable) = doMapping(truthName, groundTruthTable, (rowsCount * .1).toInt, shuffle = true)

      val queryName = s"query_$concept${configs.queryRowsCount}"
      csvUtils.exportTable(queryTable, queryName)
      val retrievedName = s"retrieved_$concept${configs.queryRowsCount}"
      csvUtils.exportTable(retrievedTable, retrievedName)

      // save
      (randomTable.docId, (truthName, queryName, retrievedName), (randomTable.columns.length, randomTable.hdrIdx, randomTable.keyIdx))
    }
    serializer.serialize(result, "100_mapping")

    result
  }

  def doMassiveExperimentEval() = {
    val mappingResult = serializer.deserialize("100_mapping").asInstanceOf[List[(Int, (String, String, String), (Int, Option[Int], Option[Int]))]]

    val result = mappingResult.map { case (docId, (truthName, queryName, retrievedName), (clmnsCount, hdrIdx, keyIdx)) =>
      val queryTable = csvUtils.importTableByName(name = queryName, configs.columnsCount, hdrIdx, keyIdx)
      val retrievedTable = csvUtils.importTableByName(name = retrievedName, configs.columnsCount, hdrIdx, keyIdx)

      val evalResult = doEval(truthName, queryTable, retrievedTable)
      (docId, evalResult)
    }
    serializer.saveAsJson(result, "100_eval.json")
    result
  }

  def getRandomTable(randomDouble: Double = util.Random.nextDouble): Table = {
    val maxDocsNum = reader.maxDoc()
    val randomDocId = (randomDouble * maxDocsNum).toInt
    val jsonTable = reader.document(randomDocId).get("raw")

    transformer.rawJsonToTable(randomDocId, jsonTable) match {
      case table if table.columns.length > 1 && table.columns.head.length > 7 =>
        table
      case _ =>
        getRandomTable()
    }

  }

  def doMapping(truthDataName: String, groundTruthTable: Table, queryRowsCount: Int, shuffle: Boolean): (Table, Table) = {
    val queryTableColumns = Table.getColumnsWithRandomRows(count=queryRowsCount, groundTruthTable, shuffle)
    val queryTable = new Table(docId = 0,"Query", "None", keyIdx = Some(0), hdrIdx = Some(0), columns = queryTableColumns)

    val tableColumnsRelations = configs.columnsRelations.map(rel => TableColumnsRelation(rel))

    println("Start")
    val startTime = System.nanoTime

    val algorithm = new TrexAlgorithm(reader, tableSearch, analyzer, truthDataName, tableColumnsRelations, configs.scoringMethod, configs.maxK)
    val retrievedTable = algorithm.run(queryTable)

    val endTime = System.nanoTime
    val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
    println(s"Finished indexing for $truthDataName. Total found ${retrievedTable.columns.head.length} in $duration seconds")

    (queryTable, retrievedTable)
  }

  def doEval(truthDataName: String, queryTable: Table, retrievedTable: Table): EvaluationResult = {

    val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.entities)
    val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

    val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(reader, IndexFields.content)
    val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

    val evaluator = new Evaluator(groundTruthTable, keySearcher, valueSearcher, truthDataName)

    val evalColumns = queryTable.columns.zipWithIndex.map { case (clmn, clmnIdx) =>
      clmn ::: retrievedTable.columns(clmnIdx)
    }
    val evalTable = Table(
      docId = 0,
      title = "eval",
      url = "no",
      keyIdx = Some(0),
      hdrIdx = None,
      columns = evalColumns
    )

    evaluator.evaluate(evalTable)

  }

  val a = 10

}
