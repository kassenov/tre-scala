package pipes.mapping

import models.relation.TableColumnsRelation
import models.{MappingPipeResult, Table}
import search.{KeySearcher, ValueSearcher}
import utls.CsvUtils

class MappingPipe(keySearcher: KeySearcher, valueSearcher: ValueSearcher) {

  val tableMatchingExtractor = new TableMatchingExtractor(keySearcher, valueSearcher)
  val tableMatchMatrixExtractor = new TableMatchMatrixExtractor()
  val tableMappingExtractor = new TableMappingExtractor()
  val tableCandidateKeysWithIndexesExtractor = new TableCandidateKeysWithIndexesExtractor()

  val excludeTables: List[String] = List.empty //List("Players - A to Z – UEFA.com")

  def process(queryTable: Table, candidateTable: Table, tableColumnsRelations: List[TableColumnsRelation]): Option[MappingPipeResult] = {

    if (excludeTables.contains(candidateTable.title)) {
      None
    } else {
      val tableMatch = tableMatchingExtractor.extract(queryTable, candidateTable)
      if (tableMatch.keyMatches.isEmpty) {
        None
      } else {
        val matchMatrix = tableMatchMatrixExtractor.extract(queryTable, tableMatch, tableColumnsRelations)

        if (!matchMatrix.columns.exists(c => c.cells.nonEmpty)) {
          None
        } else {
          val columnsMapping = tableMappingExtractor.extract(matchMatrix, tableMatch)
          val candidateKeysWithIndexes = tableCandidateKeysWithIndexesExtractor.extract(candidateTable, tableMatch)

          Some(MappingPipeResult(columnsMapping, candidateKeysWithIndexes, tableMatch))
        }

      }
    }

  }

}
