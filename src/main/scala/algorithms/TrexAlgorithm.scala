package algorithms

import models.{MappingPipeResult, Table}
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.filtering._
import pipes.mapping._
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import transformers.Transformer

class TrexAlgorithm(indexReader: IndexReader, analyzer: Analyzer) extends Algorithm {

  private val transformer = new Transformer

  // Searchers

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.entities)
  private val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.content)
  private val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

  // Pipes

  private val mappingPipe = new MappingPipe(keySearcher, valueSearcher)

  //private var usedTables: List[Table] = _

  val sizeFilter = new FilterTableBySize(minRows = 5, minCols = 3)
  val candidateKeysFilter = new FilterTableByCandidateKeys()

  override def run(queryTable: Table, tableSearcher: TableSearcher): List[List[String]] = {

    val mappingResults =
      tableSearcher
        .getRawJsonTablesByKeys(Table.getKeys(queryTable))
        .par
        .map { jsonTable => transformer.rawJsonToTable(jsonTable) }
        .filter { candidateTable => sizeFilter.apply(candidateTable) }
        .map { candidateTable => mappingPipe.process(queryTable, candidateTable) }
        .filter { mappingResult => candidateKeysFilter.apply(mappingResult.candidateKeys) }



  }


}
