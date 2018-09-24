import java.io.File
import java.util.concurrent.TimeUnit

import algorithms.TrexAlgorithm
import models.Table
import models.relation.TableColumnsRelation
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import search.LuceneTableSearcher
import utls.CsvUtils


object Main extends App {
  val rootPath = "/media/light/SSD/50"
  val concept = "countries"

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)

  var tableSearch = new LuceneTableSearcher(searcher)

  val analyzer = new StandardAnalyzer
  val algorithm = new TrexAlgorithm(reader, analyzer)

  //====================================================
  val csvUtils = new CsvUtils()
  val groundTruthTable = csvUtils.importTable(name = "truth_countries", clmnsCount = 6, hdrRowIdx = None)

  val queryTableColumns = Table.getRandomColumns(count=10, groundTruthTable)
  val queryTable = new Table("Query", "None", keyIdx = Some(0), hdrIdx = None, columns = queryTableColumns)

  //====================================================

  val tableColumnsRelations = List(TableColumnsRelation(List(0, 1)), TableColumnsRelation(List(0, 2)))

  //====================================================

  println("Start")
  val startTime = System.nanoTime

  val retrievedTable = algorithm.run(queryTable, tableSearch, tableColumnsRelations)

  val endTime = System.nanoTime
  val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
  println(s"Finished indexing for $concept. Total found ${retrievedTable.columns.length} in $duration seconds")

  csvUtils.exportTable(retrievedTable, concept)

  val a = 10

}
