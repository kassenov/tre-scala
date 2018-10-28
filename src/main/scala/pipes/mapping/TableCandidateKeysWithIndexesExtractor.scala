package pipes.mapping

import models.Table
import models.matching.{KeyWithIndex, TableMatch}

class TableCandidateKeysWithIndexesExtractor() {

  def extract(tableKeys: List[Option[String]], tableMatching: TableMatch): List[KeyWithIndex] = {

    val excludeIdxes = tableMatching.keyMatches.flatten { keyMatch =>
      keyMatch.rowMatches.map(rowMatch => rowMatch.candidateRowId)
    }

    tableKeys
      .zipWithIndex
      .flatMap {
        case (key, idx) if key.isDefined && !excludeIdxes.contains(idx) => Some(KeyWithIndex(key.get.toLowerCase, idx))
        case _ => None
      }

  }

}
