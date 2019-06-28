package pipes.mapping

import models.matching.{KeyWithIndex, TableMatch}

class QueryKeysWithIndexesExtractor() {

  def extract(tableKeys: List[Option[String]], tableMatching: TableMatch): List[KeyWithIndex] = {

    val includeIdxes = tableMatching.keyMatches.flatten { keyMatch =>
      keyMatch.rowMatches.map(rowMatch => rowMatch.candidateRowId)
    }

    tableKeys
      .zipWithIndex
      .flatMap {
        case (key, idx) if key.isDefined && includeIdxes.contains(idx) => Some(KeyWithIndex(key.get.toLowerCase, idx))
        case _ => None
      }

  }

}
