package algorithms

import models.{MappingPipesResult, Table}
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.filtering.{TableMappingPipe, TableMatchingMatrixExtractingPipe, TableMatchingPipe}
import pipes.mapping.{TableCandidateKeysExtractingPipe, TableMappingPipe, TableMatchingMatrixExtractingPipe, TableMatchingPipe}
import pipes.{TableMappingPipe, TableMatchingMatrixExtractingPipe, TableMatchingPipe}
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import transformers.Transformer

class TrexAlgorithm(indexReader: IndexReader, analyzer: Analyzer) extends Algorithm {

  private val transformer = new Transformer

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.entities)
  private val keySearch = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.content)
  private val valueSearch = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

  private var usedTables: List[Table] = _

  override def run(queryTable: Table, tableSearcher: TableSearcher): List[List[String]] = {

    tableSearcher.getRawJsonTablesByKeys(Table.getKeys(queryTable))
      .par
      .map { jsonTable => transformer.rawJsonToTable(jsonTable) }
      .map { candidateTable =>
        processByMappingPipes(queryTable, candidateTable)
      }

  }

  val tableMatchingPipe = new TableMatchingPipe(keySearch, valueSearch)
  val tableMatchingMatrixExtractingPipe = new TableMatchingMatrixExtractingPipe()
  val tableMappingPipe = new TableMappingPipe()
  val tableCandidateKeysExtractingPipe = new TableCandidateKeysExtractingPipe()

  private def processByMappingPipes(queryTable: Table, candidateTable: Table): MappingPipesResult = {

    val tableMatching = tableMatchingPipe.process(queryTable, candidateTable)
    val matchingMatrix = tableMatchingMatrixExtractingPipe.process(tableMatching)
    val columnsMapping = tableMappingPipe.process(matchingMatrix)
    val candidateKeys = tableCandidateKeysExtractingPipe.process(candidateTable, tableMatching)

    MappingPipesResult(candidateTable, columnsMapping, candidateKeys)

  }

}
