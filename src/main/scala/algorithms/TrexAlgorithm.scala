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
    super.initTimings(levels = 3)

    val queryKeys = Table.getKeys(queryTable)
    val queryColumnsCount = queryTable.columns.length

    // Mapping candidate tables
    println(s"===== Started searching for candidate tables =====")

    val candidateDocIdToMappingResult = deserializeOrFindAndMapByQueryKeysAndDataName(queryTable, dataName)

    reportWithDuration(level = 1, s"Total ${candidateDocIdToMappingResult.toList.length} candidate tables")

    // Candidate keys
    println(s"===== Started extracting candidate keys =====")

    val candidateKeyToCandidateDocIds = getCandidateKeyToCandidateDocIdsMap(candidateDocIdToMappingResult)

    val candidateKeys = candidateKeyToCandidateDocIds.keys

    reportWithDuration(level = 1, s"Total ${candidateKeyToCandidateDocIds.toList.length} candidate keys")

    // Query keys
    println(s"===== Started associating query keys =====")
    // table to query keys

    val queryKeyToCandidateDocIds = getQueryKeyToCandidateDocIds(queryKeys, candidateDocIdToMappingResult)

    reportWithDuration(level = 1, s"Total ${queryKeyToCandidateDocIds.toList.length} query keys")

    // Top candidate keys
    println(s"===== Started to extract top candidate keys =====")

    val candidateKeyToQueryTableSim =
      candidateKeys.map { candidateKey =>
        val candidateKeyTables = candidateKeyToCandidateDocIds(candidateKey)

        val score = queryKeys.flatten.map { queryKey =>
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

    reportWithDuration(level = 1, s"Total ${topCandidateKeys.length} top candidate keys")

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

    reportWithDuration(level = 1, s"Selected values")
    reportWithDuration(level = 0, s"Finished")

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
      println(s"De-serializing from file...")
      serializer.deserialize(dataName).asInstanceOf[Map[Int,MappingPipeResult]]
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

  private def getCandidateKeyToCandidateDocIdsMap(candidateDocIdToMappingResult: Map[Int,MappingPipeResult]): Map[String, Set[Int]] = {
    val candidateKeyToCandidateDocIdsHashMap = mutable.HashMap[String, mutable.ListBuffer[Int]]()
    candidateDocIdToMappingResult.foreach { case (candidateDocId, mappingResult) =>
      mappingResult.candidateKeysWithIndexes.map{ keyWithIndex =>
        val key = keyWithIndex.value
        if (!candidateKeyToCandidateDocIdsHashMap.contains(key)) {
          candidateKeyToCandidateDocIdsHashMap += key -> mutable.ListBuffer[Int]()
        }
        candidateKeyToCandidateDocIdsHashMap(key) += candidateDocId
      }
    }
    candidateKeyToCandidateDocIdsHashMap.map { case (key, candidateDocIds) =>
      key -> candidateDocIds.toSet
    }.toMap
  }

  private def getQueryKeyToCandidateDocIds(queryKeys: List[Option[String]], candidateDocIdToMappingResult: Map[Int,MappingPipeResult]): Map[String, Set[Int]] = {
    val queryKeyToCandidateDocIdsHashMap = mutable.HashMap[String, mutable.ListBuffer[Int]]()
    candidateDocIdToMappingResult.foreach { case (candidateDocId, mappingResult) =>
      mappingResult.tableMatch.keyMatches.map { keyMatch =>
        queryKeys(keyMatch.queryRowIdx) match {
          case Some(key) =>
            if (!queryKeyToCandidateDocIdsHashMap.contains(key)) {
              queryKeyToCandidateDocIdsHashMap += key -> mutable.ListBuffer[Int]()
            }
            queryKeyToCandidateDocIdsHashMap(key) += candidateDocId
          case None => // Nothing
        }
      }
    }
    queryKeyToCandidateDocIdsHashMap.map { case (key, candidateDocIds) =>
      key -> candidateDocIds.toSet
    }.toMap
  }

}
