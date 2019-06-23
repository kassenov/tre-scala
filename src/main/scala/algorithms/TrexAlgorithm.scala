package algorithms

import models.{MappingPipeResult, Table}
import models.index.IndexFields
import models.relation.TableColumnsRelation
import models.selection.{CellValue, ColumnCellValues, KeyColumnsValuesModel}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.mapping._
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import thresholding.otsu
import transformers.Transformer
import utls.{MapScoring, Serializer}

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._

class TrexAlgorithm(indexReader: IndexReader,
                    tableSearcher: TableSearcher,
                    analyzer: Analyzer,
                    dataName: String,
                    tableColumnsRelations: List[TableColumnsRelation],
                    scoringMethod: MapScoring.Value,
                    topK: Int) extends Algorithm with Timing {

  private val transformer = new Transformer
  private val serializer = new Serializer()

  // Searchers

  private val entitiesTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.entities)
  private val keySearcher = new KeySearcherWithSimilarity(entitiesTermFrequencyProvider, analyzer)

  private val contentTermFrequencyProvider = new LuceneIndexTermFrequencyProvider(indexReader, IndexFields.content)
  private val valueSearcher = new ValueSearcherWithSimilarity(contentTermFrequencyProvider, analyzer)

  //private var usedTables: List[Table] = _

  override def run(queryTable: Table): Table = {
    super.initTimings(levels = 3)

    val queryKeys = Table.getKeys(queryTable).distinct
    val queryColumnsCount = queryTable.columns.length

    // Mapping candidate tables
    println(s"===== Started searching for candidate tables =====")

    val mappingPipe = new MappingPipe(keySearcher, valueSearcher, tableSearcher, tableColumnsRelations, queryTable, dataName, scoringMethod)
    val docIdToMappingResult = mappingPipe.deserializeOrFindAndMapByQueryKeysAndDataName()

    reportWithDuration(level = 1, s"Total ${docIdToMappingResult.toList.length} candidate tables")

    // Candidate keys
    println(s"===== Started extracting candidate keys =====")

    val candidateKeyToDocIds = getCandidateKeyToCandidateDocIdsMap(docIdToMappingResult)

    val candidateKeys = candidateKeyToDocIds.keys.toList

    reportWithDuration(level = 1, s"Total ${candidateKeyToDocIds.toList.length} candidate keys")

    // Query keys
    println(s"===== Started associating query keys =====")
    // table to query keys

    val queryKeyToDocIds = getQueryKeyToCandidateDocIds(queryKeys, docIdToMappingResult)

    reportWithDuration(level = 1, s"Total ${queryKeyToDocIds.toList.length} query keys")

    // Top candidate keys
    println(s"===== Started to extract top candidate keys =====")

    val topCandidateKeys = getTopCandidateKeys(candidateKeys, candidateKeyToDocIds, queryKeys, queryKeyToDocIds, docIdToMappingResult, queryColumnsCount, topK)
    val keyToScore = getCandidateKeysToSim(candidateKeys, candidateKeyToDocIds, queryKeys, queryKeyToDocIds, docIdToMappingResult, queryColumnsCount)

    reportWithDuration(level = 1, s"Total ${topCandidateKeys.length} top candidate keys")

    // Value
    println(s"===== Started selecting values for candidate keys =====")

    val records = buildRecords(topCandidateKeys, candidateKeyToDocIds, docIdToMappingResult, queryColumnsCount)

    val columns = List.range(0, queryColumnsCount).map { clmIdx =>
      records.map(r => r(clmIdx))
    }

    reportWithDuration(level = 1, s"Selected values")
    reportWithDuration(level = 0, s"Finished")

    exportPreBuildRecordsData(queryKeys, candidateKeys, candidateKeyToDocIds, docIdToMappingResult, queryColumnsCount, dataName, keyToScore)

    Table(
      docId = -1,
      title = "retrieved",
      url = "no",
      keyIdx = Some(0),
      hdrIdx = None,
      columns = columns
    )

  }

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
    val result = candidateKeyToCandidateDocIdsHashMap.map { case (key, candidateDocIds) =>
      key -> candidateDocIds.toSet
    }.toMap

    serializer.saveAsJson(result, s"${dataName}_CandidateKeyToCandidateDocIds")

    result
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
    val result = queryKeyToCandidateDocIdsHashMap.map { case (key, candidateDocIds) =>
      key -> candidateDocIds.toSet
    }.toMap

    serializer.saveAsJson(result, s"${dataName}_QueryKeyToCandidateDocIds")

    result
  }

  private def getTopCandidateKeys(candidateKeys: List[String],
                                  candidateKeyToDocIds: Map[String, Set[Int]],
                                  queryKeys: List[Option[String]],
                                  queryKeyToCandidateDocIds: Map[String, Set[Int]],
                                  docIdToMappingResult: Map[Int,MappingPipeResult],
                                  clmnsCount: Int,
                                  topK: Int): List[String] = {
    val candidateKeyToQueryTableSim =
      candidateKeys.map { candidateKey =>
        val candidateKeyDocIds = candidateKeyToDocIds(candidateKey)

        val score = queryKeys.flatten.map { queryKey =>

          if (queryKeyToCandidateDocIds.contains(queryKey)) {
            val queryKeyDocIds = queryKeyToCandidateDocIds(queryKey)

            // FIXME Update scoring for candidate keys (it's relevance only, we need coherence too)

            val unionScore = queryKeyDocIds.union(candidateKeyDocIds)
              .map { candidateDocId =>
                docIdToMappingResult(candidateDocId).columnsMapping.score
                  .aggregatedByColumns.score / clmnsCount.toDouble
              }.sum

            val intersectionScore = queryKeyDocIds.intersect(candidateKeyDocIds)
              .map { candidateDocId =>
                docIdToMappingResult(candidateDocId).columnsMapping.score
                  .aggregatedByColumns.score / clmnsCount.toDouble
              }.sum

            intersectionScore.toDouble / unionScore.toDouble
          } else {
            0
          }

        }.sum / queryKeys.size.toDouble

        candidateKey -> score

      }

    val nonNan = candidateKeyToQueryTableSim.filter { case (_, score) => !score.isNaN }

    val similarities = candidateKeyToQueryTableSim.toMap.map{ case (_, score) => score * 10 }.toList
    val topKeys = (if (topK > 0) {
      candidateKeyToQueryTableSim
        .sortBy { case (_, score) => score }
        .reverse
        .take(topK)
    } else {
      val threshold = otsu.getThreshold(similarities) / 30

      candidateKeyToQueryTableSim
        .filter { case (_, score) => score >= threshold }
    }).map { case (key, _) => key }

    topKeys
  }

  private def getCandidateKeysToSim(candidateKeys: List[String],
                                    candidateKeyToDocIds: Map[String, Set[Int]],
                                    queryKeys: List[Option[String]],
                                    queryKeyToCandidateDocIds: Map[String, Set[Int]],
                                    docIdToMappingResult: Map[Int,MappingPipeResult],
                                    clmnsCount: Int): Map[String, Double] =
      candidateKeys.map { candidateKey =>
        val candidateKeyDocIds = candidateKeyToDocIds(candidateKey)

        val score = queryKeys.flatten.map { queryKey =>
          if (queryKeyToCandidateDocIds.contains(queryKey)) {
            val queryKeyDocIds = queryKeyToCandidateDocIds(queryKey)

            // FIXME Update scoring for candidate keys (it's relevance only, we need coherence too)

            val unionScore = queryKeyDocIds.union(candidateKeyDocIds)
              .map { candidateDocId =>
                docIdToMappingResult(candidateDocId).columnsMapping.score
                  .aggregatedByColumns.score / clmnsCount
              }.sum

            val intersectionScore = queryKeyDocIds.intersect(candidateKeyDocIds)
              .map { candidateDocId =>
                docIdToMappingResult(candidateDocId).columnsMapping.score
                  .aggregatedByColumns.score / clmnsCount
              }.sum

            intersectionScore / unionScore
          }
          else {
            0
          }

        }.sum / queryKeys.size

        candidateKey -> score

      }.toMap

  private def buildRecords(keys: List[String],
                           keyToDocIds: Map[String, Set[Int]],
                           docIdToMappingResult: Map[Int,MappingPipeResult],
                           clmnsCount: Int): List[List[Option[String]]] =
    keys.par.map { key =>

      val docIds = keyToDocIds(key)
      val values = List.range(1, clmnsCount).map { queryClmIdx =>
        val candValueToScoreMap = mutable.Map[String, Double]()

        docIds.foreach { docId =>
          val mappingResult = docIdToMappingResult(docId)
          val rowIdx = mappingResult.candidateKeysWithIndexes.find(c => c.value == key).get.idx

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

      Some(key) :: values
    }.toList

  private def exportPreBuildRecordsData(queryKeys: List[Option[String]],
                                        keys: List[String],
                                        keyToDocIds: Map[String, Set[Int]],
                                        docIdToMappingResult: Map[Int,MappingPipeResult],
                                        clmnsCount: Int,
                                        dataName: String,
                                        keyToScore: Map[String, Double]): Unit = {
    val data = getPreBuildRecordsData(queryKeys.flatten.map(k => k.toLowerCase()) ++ keys, keyToDocIds, docIdToMappingResult, clmnsCount, keyToScore)

    serializer.saveAsJson(data, s"${dataName}_PreBuildsRecordsData")

  }

  private def getPreBuildRecordsData(keys: List[String],
                                     keyToDocIds: Map[String, Set[Int]],
                                     docIdToMappingResult: Map[Int,MappingPipeResult],
                                     clmnsCount: Int,
                                     keyToScore: Map[String, Double]): Seq[(String, KeyColumnsValuesModel)] = {

    keys.par.map { key =>

      if (keyToDocIds.contains(key)) {
        val docIds = keyToDocIds(key)
        val columnsWithValues = List.range(1, clmnsCount).map { queryClmIdx =>

          val candValueToScoreMap = mutable.Map[String, Double]()
          val candValueToDocIdsMap = mutable.Map[String, mutable.ListBuffer[Int]]()

          docIds.foreach { docId =>
            val mappingResult = docIdToMappingResult(docId)
            val rowIdx = mappingResult.candidateKeysWithIndexes.find(c => c.value == key).get.idx

            val jsonTable = tableSearcher.getRawJsonTableByDocId(docId)
            val table = transformer.rawJsonToTable(docId, jsonTable)
            mappingResult.columnsMapping.columnIdxes(queryClmIdx) match {
              case Some(candClmIdx) if table.columns(candClmIdx)(rowIdx).isDefined =>
                val value = table.columns(candClmIdx)(rowIdx).get.toLowerCase
                val score = mappingResult.columnsMapping.score.columns(queryClmIdx).get.score
                if (!candValueToScoreMap.contains(value)) {
                  candValueToScoreMap(value) = score
                  candValueToDocIdsMap(value) = mutable.ListBuffer[Int](docId)
                } else {
                  candValueToScoreMap(value) = candValueToScoreMap(value) + score
                  candValueToDocIdsMap(value) += docId
                }
              case None             => //
            }
          }

          val cellValues = candValueToScoreMap.keys.map { value =>
            CellValue(
              value = value,
              relevance = 0,
              coherence = 0,
              score = candValueToScoreMap(value),
              docIds = candValueToDocIdsMap(value).toSet
            )
          }

          val selectedValue = if (candValueToScoreMap.isEmpty) {
            None
          } else {
            Some(candValueToScoreMap.maxBy{ case (_, score) => score }._1)
          }

          ColumnCellValues(
            clmnIdx = queryClmIdx,
            selectedValue = selectedValue,
            values = cellValues.toList
          )

        }

        key -> KeyColumnsValuesModel(keyToScore(key), columnsWithValues)
      } else {
        key -> KeyColumnsValuesModel(0, List.empty)
      }

    }.seq.sortBy {
      case (_, model) => model.score
    }

  }

}
