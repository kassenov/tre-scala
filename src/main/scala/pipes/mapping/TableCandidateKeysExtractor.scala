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
      .flatMap {
        case (key, idx) if !excludeIdxes.contains(idx) => Some(KeyWithIndex(key, idx))
        case _ => None
      }

  }

}
