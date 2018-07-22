package algorithms

import models.Table
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
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

    val tableMatchingPipe = new TableMatchingPipe(queryTable, keySearch, valueSearch)
    val tableMatchingMatrixExtractingPipe = new TableMatchingMatrixExtractingPipe()
    val tableMappingPipe = new TableMappingPipe()

    tableSearcher.getRawJsonTablesByKeys(Table.getKeys(queryTable))
      .par
      // Formatting
      .map { jsonTable => transformer.rawJsonToTable(jsonTable) }
      // TableMatching
      .map { candidateTable =>
        (candidateTable, tableMatchingPipe.process(candidateTable))
      }
      // MatchingMatrix
      .map { case (candidateTable, tableMatching) =>
        (candidateTable, tableMatchingMatrixExtractingPipe.process(tableMatching))
      }
      // TableMapping
      .map { case (candidateTable, matchingMatrix) =>
        (candidateTable, tableMappingPipe.process(matchingMatrix))
      }

  }

}
