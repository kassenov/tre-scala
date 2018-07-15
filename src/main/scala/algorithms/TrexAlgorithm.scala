package algorithms

import models.Table
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.{TableMappingPipe, TableMatchingMatrixExtractingPipe, TableMatchingPipe}
import search.{KeySearchWithSimilarity, TableSearch, ValueSearchWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import transformers.Transformer

class TrexAlgorithm(indexReader: IndexReader, analyzer: Analyzer) extends Algorithm {

  private val transformer = new Transformer

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.entities)
  private val keySearch = new KeySearchWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.content)
  private val valueSearch = new ValueSearchWithSimilarity(contentTermFrequencyProvider, analyzer)

  private var usedTables: List[Table] = _

  override def run(queryTable: Table, tableSearch: TableSearch): List[List[String]] = {

    val tableMatchingPipe = new TableMatchingPipe(queryTable, keySearch, valueSearch)
    val tableMatchingMatrixExtractingPipe = new TableMatchingMatrixExtractingPipe()
    val tableMappingPipe = new TableMappingPipe()

    tableSearch.getRawJsonTablesByKeys(Table.getKeys(queryTable))
      .par
      .map(jsonTable => transformer.rawJsonToTable(jsonTable))
      .map(candidateTable => tableMatchingPipe.process(candidateTable))
      .map(tableMatching => tableMappingPipe.process(tableMatching))

  }

}
