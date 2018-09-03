package pipes.mapping

import models.Table
import models.matching.{KeyWithIndex, TableMatch}

class TableCandidateKeysExtractor() {

  def extract(table: Table, tableMatching: TableMatch): List[KeyWithIndex] = {

    val excludeIdxes = tableMatching.keyMatches.flatten { keyMatch =>
      keyMatch.rowMatches.map(rowMatch => rowMatch.candidateRowId)
    }

    Table.getKeys(table)
      .zipWithIndex
      .flatMap {
        case (key, idx) if !excludeIdxes.contains(idx) => Some(KeyWithIndex(key, idx))
        case _ => None
      }

  }

}
