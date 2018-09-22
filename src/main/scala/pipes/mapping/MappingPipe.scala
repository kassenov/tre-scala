package pipes.mapping

import models.{MappingPipeResult, Table}
import search.{KeySearcher, ValueSearcher}

class MappingPipe(keySearcher: KeySearcher, valueSearcher: ValueSearcher) {

  val tableMatchingExtractor = new TableMatchingExtractor(keySearcher, valueSearcher)
  val tableMatchMatrixExtractor = new TableMatchMatrixExtractor()
  val tableMappingExtractor = new TableMappingExtractor()
  val tableCandidateKeysWithIndexesExtractor = new TableCandidateKeysWithIndexesExtractor()

  def process(queryTable: Table, candidateTable: Table): Option[MappingPipeResult] = {

    val tableMatch = tableMatchingExtractor.extract(queryTable, candidateTable)
    if (tableMatch.keyMatches.isEmpty) {
      None
    } else {
      val matchMatrix = tableMatchMatrixExtractor.extract(tableMatch)
      val columnsMapping = tableMappingExtractor.extract(matchMatrix, tableMatch)
      val candidateKeysWithIndexes = tableCandidateKeysWithIndexesExtractor.extract(candidateTable, tableMatch)

      Some(MappingPipeResult(columnsMapping, candidateKeysWithIndexes, tableMatch))
    }

  }

}
