package pipes.mapping

import models.Table
import models.matching.{KeyWithIndex, TableMatching}

class TableCandidateKeysExtractor() {

  def extract(table: Table, tableMatching: TableMatching): List[KeyWithIndex] = {

    val excludeIdxes = tableMatching.keyMatches.flatten { keyMatch =>
      keyMatch.rowMatchings.map(rowMatch => rowMatch.candidateRowId)
    }

    Table.getKeys(table)
      .zipWithIndex
      .map {
        case (key, idx) if !excludeIdxes.contains(idx) => KeyWithIndex(key, idx)
        case _ => None
      }.flatten

  }

}
