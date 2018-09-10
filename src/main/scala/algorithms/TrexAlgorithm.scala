package algorithms

import models.Table
import models.index.IndexFields
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.filtering._
import pipes.mapping._
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import thresholding.otsu
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

  // Filters

  val sizeFilter = new FilterTableBySize(minRows = 5, minCols = 3)
  val candidateKeysFilter = new FilterTableByCandidateKeys()

  override def run(queryTable: Table, tableSearcher: TableSearcher): List[List[String]] = {

    val queryKeys = Table.getKeys(queryTable)

    // Mapping candidate tables

    val candidateTableToMappingResult =
      tableSearcher
        .getRawJsonTablesByKeys(queryKeys)
        .par
        .map { jsonTable => transformer.rawJsonToTable(jsonTable) }
        .filter { candidateTable => sizeFilter.apply(candidateTable) }
        .flatMap { candidateTable =>
          val mappingResult = mappingPipe.process(queryTable, candidateTable)
          if (mappingResult.isDefined) {
            Some(candidateTable -> mappingResult.get)
          } else {
            None
          }
        }
        .filter { case (_, mappingResult) => candidateKeysFilter.apply(mappingResult.candidateKeysWithIndexes) }
      .toMap



    // Candidate keys

    // table to candidate keys
    val candidateTableToCandidateKeys =
      candidateTableToMappingResult.map { case (candidateTable, mappingResult) =>
        candidateTable -> mappingResult.candidateKeysWithIndexes.map(keyWithIndex => keyWithIndex.value)
      }

    // candidate keys
    val candidateKeys = candidateTableToCandidateKeys.flatten { case (_, keys) => keys }.toList.toSet

    // candidate key to table
    val candidateKeyToCandidateTables =
      candidateKeys.map { key =>
        val candidateTables = candidateTableToCandidateKeys
          .filter{ case (_, keys) => keys.contains(key) }
          .map{ case (candidateTable, _) => candidateTable }

        key -> candidateTables.toSet
      }.toMap

    // Query keys

    // table to query keys
    val candidateTableToQueryKeys =
      candidateTableToMappingResult.map { case (candidateTable, mappingResult) =>
          candidateTable -> mappingResult.tableMatch.keyMatches.map(keyMatch => queryKeys(keyMatch.queryRowIdx))
      }

    // query key to table
    val queryKeyToCandidateTables =
      queryKeys.map { key =>
        val candidateTables = candidateTableToQueryKeys
          .filter{ case (_, keys) => keys.contains(key) }
          .map{ case (candidateTable, _) => candidateTable }

        key -> candidateTables.toSet
      }.toMap

    // Top candidate keys

    val candidateKeyToQueryTableSim =
      candidateKeys.map { candidateKey =>
        val candidateKeyTables = candidateKeyToCandidateTables(candidateKey)

        val score = queryKeys.map { queryKey =>
          val queryKeyTables = queryKeyToCandidateTables(queryKey)

          // TODO Update scoring for candidate keys

          val unionScore = queryKeyTables.union(candidateKeyTables)
            .map { candidateTable =>
              candidateTableToMappingResult(candidateTable).columnsMapping.score
                .aggregatedByColumns.score / queryTable.columns.size
            }.sum

          val intersectionScore = queryKeyTables.intersect(candidateKeyTables)
            .map { candidateTable =>
              candidateTableToMappingResult(candidateTable).columnsMapping.score
                .aggregatedByColumns.score / queryTable.columns.size
            }.sum

          intersectionScore / unionScore

        }.sum / queryKeys.size

        candidateKey -> score

      }

    val similarities = candidateKeyToQueryTableSim.toMap.map{ case (_, score) => score * 100 }.toList
    val threshold = otsu.getThreshold(similarities) / 100

    val topCandidateKeys = candidateKeyToQueryTableSim
      .filter { case (_, score) => score >= threshold }
      .map { case (key, _) => key }
      .toList


//    val sortedCandidateKeyToQueryTableSim = candidateKeyToQueryTableSim.toList.sortBy{ case (key, score) => score }

    // TODO Getting top
//    val topCandidateKeys = sortedCandidateKeyToQueryTableSim.map { case (key, score) => key }

    List.empty

  }


}
