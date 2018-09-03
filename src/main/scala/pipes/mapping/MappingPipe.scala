package pipes.mapping

import models.{MappingPipeResult, Table}
import search.{KeySearcher, ValueSearcher}

class MappingPipe(keySearcher: KeySearcher, valueSearcher: ValueSearcher) {

  val tableMatchingExtractor = new TableMatchingExtractor(keySearcher, valueSearcher)
  val tableMatchingMatrixExtractor = new TableMatchingMatrixExtractor()
  val tableMappingExtractor = new TableMappingExtractor()
  val tableCandidateKeysExtractor = new TableCandidateKeysExtractor()

  def process(queryTable: Table, candidateTable: Table): Option[MappingPipeResult] = {

    val tableMatch = tableMatchingExtractor.extract(queryTable, candidateTable)
    if (tableMatch.keyMatches.isEmpty) {
      None
    } else {
      val matchingMatrix = tableMatchingMatrixExtractor.extract(tableMatch)
      val columnsMapping = tableMappingExtractor.extract(matchingMatrix, tableMatch)
      val candidateKeys = tableCandidateKeysExtractor.extract(candidateTable, tableMatch)

      Some(MappingPipeResult(columnsMapping, candidateKeys, tableMatch))
    }

  }

}
