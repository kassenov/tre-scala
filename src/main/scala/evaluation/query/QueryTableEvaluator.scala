package evaluation.query

import algorithms.Timing
import models.{MappingPipeResult, Table}
import models.index.IndexFields
import models.relation.TableColumnsRelation
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import pipes.mapping.MappingPipe
import search.{KeySearcherWithSimilarity, TableSearcher, ValueSearcherWithSimilarity}
import statistics.LuceneIndexTermFrequencyProvider
import transformers.Transformer
import utls.{MapScoring, Serializer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._

case class rec(tid: Int, key: String, value: String)
//case class A(n: Int, recs: List[rec])

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

  val lnOf2 = scala.math.log(2)
  def log2(x: Double): Double = scala.math.log(x) / lnOf2

  def run(queryTable: Table): Table = {
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

//    val allKeys = candidateKeys ++ queryKeys.flatten

    reportWithDuration(level = 1, s"Total ${candidateKeys.length} top candidate keys")

    // Value
    println(s"===== Started selecting values for candidate keys =====")

    val idxToNtoAMap = getAs(candidateKeys, candidateKeyToDocIds, docIdToMappingResult, queryColumnsCount)

    idxToNtoAMap.foreach { case (clmnIdx, nToAMap) =>
      println(s"--- clmn idx $clmnIdx ---")

      nToAMap.foreach { case (n, a) =>

        val total = a.length
        val pairs = a.map { r =>
          (r.key, r.value)
        }.toSet

        val relFreqs = pairs.toList.map { pair =>
          val numOfPairs = a.count { r =>
            r.key == pair._1 && r.value == pair._2
          }
          numOfPairs.toDouble / total.toDouble
        }

        val p = relFreqs.map { relFreq =>
          val log2b = log2(relFreq)
          val log10b = scala.math.log(relFreq)
          relFreq * log2b
        }
        println(s"n $n sum ${-p.sum}")
      }
    }

    queryTable

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

  private def getAs(keys: List[String],
                    keyToDocIds: Map[String, Set[Int]],
                    docIdToMappingResult: Map[Int,MappingPipeResult],
                    clmnsCount: Int): Map[Int, Map[Int, List[rec]]] =

    List.range(1, clmnsCount).map { queryClmIdx =>
      val nToRecsMap = mutable.Map[Int, mutable.ListBuffer[rec]]()
      keys.par.foreach { key =>
        val docIds = keyToDocIds(key)

        docIds.foreach { docId =>
          val mappingResult = docIdToMappingResult(docId)
          val rowIdx = mappingResult.candidateKeysWithIndexes.find(c => c.value == key).get.idx

          val jsonTable = tableSearcher.getRawJsonTableByDocId(docId)
          val table = transformer.rawJsonToTable(docId, jsonTable)
          mappingResult.columnsMapping.columnIdxes(queryClmIdx) match {
            case Some(candClmIdx) if table.columns(candClmIdx)(rowIdx).isDefined =>
              val value = table.columns(candClmIdx)(rowIdx).get.toLowerCase
              val score = mappingResult.columnsMapping.score.columns(queryClmIdx).get.score
              if (!nToRecsMap.contains(score.toInt)) {
                nToRecsMap(score.toInt) = ListBuffer.empty
              }
              nToRecsMap(score.toInt).+=(rec(docId, key, value))
            case None => //
          }
        }

      }

      val a = nToRecsMap.map { case (n, recs) =>
        (n, recs.toList)
      }.toMap

      (queryClmIdx, a)

    }.toMap

}
