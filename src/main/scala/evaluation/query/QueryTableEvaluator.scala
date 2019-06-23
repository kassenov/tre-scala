package evaluation.query

import algorithms.Timing
import models.Table
import models.index.IndexFields
import models.relation.TableColumnsRelation
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.mapping.MappingPipe
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import transformers.Transformer
import utls.{MapScoring, Serializer}

class QueryTableEvaluator(indexReader: IndexReader,
                          tableSearcher: TableSearcher,
                          analyzer: Analyzer,
                          dataName: String,
                          tableColumnsRelations: List[TableColumnsRelation],
                          scoringMethod: MapScoring.Value,
                          topK: Int) extends Timing {

  private val transformer = new Transformer
  private val serializer = new Serializer()

  // Searchers

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.entities)
  private val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.content)
  private val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

  def run(queryTable: Table): Table = {
    super.initTimings(levels = 3)

    val queryKeys = Table.getKeys(queryTable).distinct
    val queryColumnsCount = queryTable.columns.length

    // Mapping candidate tables
    println(s"===== Started searching for candidate tables =====")

    val mappingPipe = new MappingPipe(keySearcher, valueSearcher, tableSearcher, tableColumnsRelations, queryTable, dataName, scoringMethod)
    val docIdToMappingResult = mappingPipe.deserializeOrFindAndMapByQueryKeysAndDataName()

    reportWithDuration(level = 1, s"Total ${docIdToMappingResult.toList.length} candidate tables")

    queryTable

  }

}
