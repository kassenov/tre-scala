package pipes.mapping

import models.{MappingPipeResult, Table}
import search.{KeySearcher, ValueSearcher}

class MappingPipe(keySearcher: KeySearcher, valueSearcher: ValueSearcher) {

  val tableMatchingExtractor = new TableMatchingExtractor(keySearcher, valueSearcher)
  val tableMatchingMatrixExtractor = new TableMatchingMatrixExtractor()
  val tableMappingExtractor = new TableMappingExtractor()
  val tableCandidateKeysExtractor = new TableCandidateKeysExtractor()

  def process(queryTable: Table, candidateTable: Table): MappingPipeResult = {

    val tableMatching = tableMatchingExtractor.process(queryTable, candidateTable)
    val matchingMatrix = tableMatchingMatrixExtractor.process(tableMatching)
    val columnsMapping = tableMappingExtractor.process(matchingMatrix)
    val candidateKeys = tableCandidateKeysExtractor.process(candidateTable, tableMatching)

    MappingPipeResult(candidateTable, columnsMapping, candidateKeys, tableMatching)

  }

}
