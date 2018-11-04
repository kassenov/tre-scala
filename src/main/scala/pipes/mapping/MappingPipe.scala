package pipes.mapping

import models.matching.TableMatch
import models.matching.matrix.MatchMatrix
import models.relation.TableColumnsRelation
import models.{MappingPipeResult, Table}
import pipes.filtering.{FilterTableByCandidateKeys, FilterTableBySize}
import search.{KeySearcher, TableSearcher, ValueSearcher}
import transformers.Transformer
import utls.{CsvUtils, Serializer}
import scala.collection.parallel.CollectionConverters._

import scala.collection.parallel.ParMap

class MappingPipe(keySearcher: KeySearcher,
                  valueSearcher: ValueSearcher,
                  tableSearcher: TableSearcher,
                  tableColumnsRelations: List[TableColumnsRelation],
                  queryTable: Table,
                  dataName: String) {

  private val serializer = new Serializer()
  private val transformer = new Transformer

  val queryKeys: List[Option[String]] = Table.getKeys(queryTable)

  val tableMatchingExtractor = new TableMatchingExtractor(keySearcher, valueSearcher)
  val tableMatchMatrixExtractor = new TableMatchMatrixExtractor()
  val tableMappingExtractor = new TableMappingExtractor()
  val tableCandidateKeysWithIndexesExtractor = new TableCandidateKeysWithIndexesExtractor()

  val excludeTables: List[String] = List.empty //List("Players - A to Z – UEFA.com")

  // Filters
  val candidateKeysFilter = new FilterTableByCandidateKeys()
  val sizeFilter = new FilterTableBySize(minRows = 5, minCols = 3)

  def deserializeOrFindAndMapByQueryKeysAndDataName(): Map[Int,MappingPipeResult] = {

    var reset = false

    // ================ Match ==================

    val matchFileName = s"${dataName}_match"
    val docIdToTableMatch = if (serializer.exists(matchFileName)) {
      println(s"De-serializing matches from file...")
      serializer.deserialize(matchFileName).asInstanceOf[Map[Int,TableMatch]]
    } else {
      val groupedDocIds = tableSearcher.getRelevantDocIdsByKeys(queryKeys).grouped(10000).toList

      val results = groupedDocIds.flatten { docIds =>

        getDocToTableMatchByDocIds(docIds)

      }.toMap

      reset = true
      serializer.serialize(results, matchFileName)

      results
    }

    // ================ Matrix ==================

    val matrixFileName = s"${dataName}_matrix"
    val docIdToMatrix = if (!reset && serializer.exists(matrixFileName)) {
      println(s"De-serializing matrices from file...")
      serializer.deserialize(matrixFileName).asInstanceOf[Map[Int,MatchMatrix]]
    } else {
      val queryColumnsCount = queryTable.columns.length

      val results = (docIdToTableMatch.par map { case (docId, tableMatch) =>
          processMatrix(tableMatch, queryColumnsCount) match {
            case Some(matrix) => Some(docId -> matrix)
            case None         => None
          }
      }).flatten.seq.toMap

      reset = true
      serializer.serialize(results, matrixFileName)

      results
    }

    // ================ Mapping ==================

    val mappingFileName = s"${dataName}_mapping"
    val docIdToMapping = if (!reset && serializer.exists(mappingFileName)) {
      println(s"De-serializing mappings from file...")
      serializer.deserialize(mappingFileName).asInstanceOf[Map[Int,MappingPipeResult]]
    } else {

      val results = (docIdToMatrix.par map { case (docId, matrix) =>
        processMapping(docIdToTableMatch(docId), matrix) match {
          case Some(mapping) =>
            val potentialKeysCount = mapping.candidateKeysWithIndexes.length
            if (potentialKeysCount > 0) {

              println(s"Found mapping for $docId with $potentialKeysCount potential keys")
              Some(docId -> mapping)

            } else {
              None
            }
          case None => None
        }
      }).flatten
        .filter { case (_, mappingResult) => candidateKeysFilter.apply(mappingResult.candidateKeysWithIndexes) }
        .seq
        .toMap

      serializer.serialize(results, mappingFileName)

      results
    }

    docIdToMapping
  }

  private def getDocToTableMatchByDocIds(docIds: List[Int]): ParMap[Int,TableMatch] =
    tableSearcher.getRawJsonTablesByDocIds(docIds)
      .par
      .map { case (docId, jsonTable) => transformer.rawJsonToTable(docId, jsonTable) }
      .filter { candidateTable => sizeFilter.apply(candidateTable) }
      .flatMap { candidateTable =>
        processMatching(candidateTable) match {
          case Some(tableMatch) => Some(candidateTable.docId -> tableMatch)
          case None             => None
        }
      }
      .toMap

  private def processMatching(candidateTable: Table): Option[TableMatch] = {

    if (excludeTables.contains(candidateTable.title)) {
      None
    } else {
      tableMatchingExtractor.extract(queryTable, candidateTable) match {
        case tableMatch if tableMatch.keyMatches.isEmpty => None
        case tableMatch => Some(tableMatch)
      }
    }

  }

  private def processMatrix(tableMatch: TableMatch, queryColumnsCount: Int): Option[MatchMatrix] = {

    tableMatchMatrixExtractor.extract(queryColumnsCount, tableMatch, tableColumnsRelations) match {
      case matchMatrix if !matchMatrix.columns.exists(c => c.cells.nonEmpty) => None
      case matchMatrix => Some(matchMatrix)
    }

  }

  private def processMapping(tableMatch: TableMatch, matchMatrix: MatchMatrix): Option[MappingPipeResult] = {

    val columnsMapping = tableMappingExtractor.extract(matchMatrix)
    val candidateKeysWithIndexes = tableCandidateKeysWithIndexesExtractor.extract(tableMatch.candidateTableKeys, tableMatch)

    Some(MappingPipeResult(columnsMapping, candidateKeysWithIndexes, tableMatch))

  }

}
