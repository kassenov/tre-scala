package algorithms

import models.Table
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.IndexReader
import pipes.{TableMappingPipe, TableMatchingPipe}
import search.{KeySearchWithSimilarity, TableSearch, ValueSearchWithSimilarity}
import transformers.Transformer

class TrexAlgorithm(indexReader: IndexReader, analyzer: EnglishAnalyzer) extends Algorithm {

  val transformer = new Transformer

  val keySearch = new KeySearchWithSimilarity(indexReader, analyzer)
  val valueSearch = new ValueSearchWithSimilarity(indexReader, analyzer)

  var usedTables: List[Table] = _

  override def run(queryTable: Table, tableSearch: TableSearch): List[List[String]] = {

    val tableMatchingPipe = new TableMatchingPipe(queryTable, keySearch, valueSearch)
    val tableMappingPipe = new TableMappingPipe()

    tableSearch.getRawJsonTablesByKeys(Table.getKeys(queryTable))
      .par
      .map(jsonTable => transformer.rawJsonToTable(jsonTable))
      .map(candidateTable => tableMatchingPipe.process(candidateTable))
      .map(tableMatching => tableMappingPipe.process(tableMatching))

  }

}
