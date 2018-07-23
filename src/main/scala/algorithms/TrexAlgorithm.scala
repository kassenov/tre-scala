package algorithms

import models.{MappingPipesResult, Table}
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.filtering._
import pipes.mapping.{TableCandidateKeysExtractor, TableMappingExtractor, TableMatchingMatrixExtractor, TableMatchingExtractor}
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

  val sizeFilter = new FilterTableBySize(minRows = 5, minCols = 3)
  val candidateKeysFilter = new FilterTableByCandidateKeys()

  override def run(queryTable: Table, tableSearcher: TableSearcher): List[List[String]] = {

    val mappingResults =
      tableSearcher
        .getRawJsonTablesByKeys(Table.getKeys(queryTable))
        .par
        .map { jsonTable => transformer.rawJsonToTable(jsonTable) }
        .filter { candidateTable => sizeFilter.filter(candidateTable) }
        .map { candidateTable => processByMappingPipe(queryTable, candidateTable) }
        .filter { mappingResult => candidateKeysFilter.filter(mappingResult.candidateKeys) }



  }

  val tableMatchingExtractor = new TableMatchingExtractor(keySearch, valueSearch)
  val tableMatchingMatrixExtractor = new TableMatchingMatrixExtractor()
  val tableMappingExtractor = new TableMappingExtractor()
  val tableCandidateKeysExtractor = new TableCandidateKeysExtractor()

  private def processByMappingPipe(queryTable: Table, candidateTable: Table): MappingPipesResult = {

    val tableMatching = tableMatchingExtractor.process(queryTable, candidateTable)
    val matchingMatrix = tableMatchingMatrixExtractor.process(tableMatching)
    val columnsMapping = tableMappingExtractor.process(matchingMatrix)
    val candidateKeys = tableCandidateKeysExtractor.process(candidateTable, tableMatching)

    MappingPipesResult(candidateTable, columnsMapping, candidateKeys, tableMatching)

  }

}
