package algorithms

import models.{MappingPipeResult, Table}
import models.index.IndexFields
import models.relation.TableColumnsRelation
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.filtering._
import pipes.mapping._
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import thresholding.otsu
import transformers.Transformer
import utls.Serializer

import scala.collection.mutable
import scala.collection.parallel.ParMap

class TrexAlgorithm(indexReader: IndexReader,
                    tableSearcher: TableSearcher,
                    analyzer: Analyzer,
                    dataName: String,
                    tableColumnsRelations: List[TableColumnsRelation]) extends Algorithm {

  private val transformer = new Transformer
  private val serializer = new Serializer()

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

  override def run(queryTable: Table): Table = {
    init()

    val queryKeys = Table.getKeys(queryTable)
    val queryColumnsCount = queryTable.columns.length

    // Mapping candidate tables
    println(s"===== Started searching for candidate tables =====")

    val candidateDocIdToMappingResult = deserializeOrFindAndMapByQueryKeysAndDataName(queryTable, dataName)

    println(s"Total ${candidateDocIdToMappingResult.toList.length} candidate tables")
    reportDuration(reset = true)

    // Candidate keys
    println(s"===== Started extracting candidate keys =====")
    // table to candidate keys
    val candidateDocIdToCandidateKeys =
      candidateDocIdToMappingResult.map { case (candidateDocId, mappingResult) =>
        candidateDocId -> mappingResult.candidateKeysWithIndexes.map(keyWithIndex => keyWithIndex.value)
      }

    // candidate keys
    val candidateKeys = candidateDocIdToCandidateKeys.flatten { case (_, keys) => keys }.toList.toSet
    println(s"Total ${candidateKeys.toList.length} candidate keys")

    // candidate key to table
    val candidateKeyToCandidateDocIds =
      candidateKeys.par.map { key =>
        val candidateDocIds = candidateDocIdToCandidateKeys
          .filter{ case (_, keys) => keys.contains(key) }
          .map{ case (candidateTable, _) => candidateTable }

        key -> candidateDocIds.toSet
      }.toMap

    reportDuration(reset = true)

    // Query keys
    println(s"===== Started associating query keys =====")
    // table to query keys
    val candidateDocIdToQueryKeys =
      candidateDocIdToMappingResult.map { case (candidateDocId, mappingResult) =>
          candidateDocId -> mappingResult.tableMatch.keyMatches.map(keyMatch => queryKeys(keyMatch.queryRowIdx))
      }

    // query key to table // TODO Log here
    val queryKeyToCandidateDocIds =
      queryKeys.map { key =>
        val candidateDocIds = candidateDocIdToQueryKeys
          .filter{ case (_, keys) => keys.contains(key) }
          .map{ case (candidateDocId, _) => candidateDocId }

        key -> candidateDocIds.toSet
      }.toMap

    reportDuration(reset = true)

    // Top candidate keys
    println(s"===== Started to extract top candidate keys =====")

    val candidateKeyToQueryTableSim =
      candidateKeys.map { candidateKey =>
        val candidateKeyTables = candidateKeyToCandidateDocIds(candidateKey)

        val score = queryKeys.map { queryKey =>
          val queryKeyDocIds = queryKeyToCandidateDocIds(queryKey)

          // TODO Update scoring for candidate keys

          val unionScore = queryKeyDocIds.union(candidateKeyTables)
            .map { candidateDocId =>
              candidateDocIdToMappingResult(candidateDocId).columnsMapping.score
                .aggregatedByColumns.score / queryTable.columns.size
            }.sum

          val intersectionScore = queryKeyDocIds.intersect(candidateKeyTables)
            .map { candidateDocId =>
              candidateDocIdToMappingResult(candidateDocId).columnsMapping.score
                .aggregatedByColumns.score / queryTable.columns.size
            }.sum

          intersectionScore / unionScore

        }.sum / queryKeys.size

        candidateKey -> score

      }

    val similarities = candidateKeyToQueryTableSim.toMap.map{ case (_, score) => score * 10 }.toList
    val threshold = otsu.getThreshold(similarities) / 10

    val topCandidateKeys = candidateKeyToQueryTableSim
      .filter { case (_, score) => score >= threshold }
      .map { case (key, _) => key }
      .toList

    reportDuration(reset = true)

    // Value
    println(s"===== Started selecting values for candidate keys =====")

    val records =
      topCandidateKeys.par.map { candidateKey =>

        val docIds = candidateKeyToCandidateDocIds(candidateKey)
        val values = List.range(1, queryColumnsCount).map { queryClmIdx =>
          val candValueToScoreMap = mutable.Map[String, Double]()

          docIds.foreach { docId =>
            val mappingResult = candidateDocIdToMappingResult(docId)
            val rowIdx = mappingResult.candidateKeysWithIndexes.find(c => c.value == candidateKey).get.idx

            val jsonTable = tableSearcher.getRawJsonTableByDocId(docId)
            val table = transformer.rawJsonToTable(docId, jsonTable)
            mappingResult.columnsMapping.columnIdxes(queryClmIdx) match {
              case Some(candClmIdx) if table.columns(candClmIdx)(rowIdx).isDefined =>
                val value = table.columns(candClmIdx)(rowIdx).get.toLowerCase
                val score = mappingResult.columnsMapping.score.columns(queryClmIdx).get.score
                if (!candValueToScoreMap.contains(value)) {
                  candValueToScoreMap(value) = score
                } else {
                  candValueToScoreMap(value) = candValueToScoreMap(value) + score
                }
              case None             => //
            }
          }

          if (candValueToScoreMap.isEmpty) {
            None
          } else {
            Some(candValueToScoreMap.maxBy{ case (_, score) => score }._1)
          }
        }

        Some(candidateKey) :: values
      }.toList

    val columnsCount = queryTable.columns.length
    val columns = List.range(0, columnsCount).map { clmIdx =>
      records.map(r => r(clmIdx))
    }

    reportDuration(reset = true)

    Table(
      docId = -1,
      title = "retrieved",
      url = "no",
      keyIdx = Some(0),
      hdrIdx = None,
      columns = columns
    )

  }

  private def deserializeOrFindAndMapByQueryKeysAndDataName(queryTable: Table, dataName: String): Map[Int,MappingPipeResult] =
    if (serializer.exists(dataName)) {
      serializer.deserialize(dataName).asInstanceOf[Map[Int,MappingPipeResult]]
      println(s"De-serialized from file...")
    } else {
      val queryKeys = Table.getKeys(queryTable)
      val groupedDocIds = tableSearcher.getRelevantDocIdsByKeys(queryKeys).grouped(10000).toList

      val results = groupedDocIds.flatten { docIds =>

        mapToQueryByDocIdsAndQueryTable(docIds, queryTable)

      }.toMap

      serializer.serialize(results, dataName)

      results
    }

  private def mapToQueryByDocIdsAndQueryTable(docIds: List[Int], queryTable: Table): ParMap[Int,MappingPipeResult] =
    tableSearcher.getRawJsonTablesByDocIds(docIds)
      .par
      .map { case (docId, jsonTable) => transformer.rawJsonToTable(docId, jsonTable) }
      .filter { candidateTable => sizeFilter.apply(candidateTable) }
      .flatMap { candidateTable =>
        val mappingResult = mappingPipe.process(queryTable, candidateTable, tableColumnsRelations)
        if (mappingResult.isDefined) {
          val potentialKeysCount = mappingResult.get.candidateKeysWithIndexes.length
          if (potentialKeysCount > 0) {

            println(s"Found mapping for ${candidateTable.docId} ${candidateTable.title} with $potentialKeysCount potential keys")
            Some(candidateTable.docId -> mappingResult.get)

          } else {
            None
          }

        } else {
          None
        }
      }
      .filter { case (_, mappingResult) => candidateKeysFilter.apply(mappingResult.candidateKeysWithIndexes) }
      .toMap


}
