package pipes.mapping

import models.matching.{KeyWithIndex, TableMatch}

class QueryKeysWithIndexesExtractor() {

  def extract(tableKeys: List[Option[String]], tableMatching: TableMatch, queryKeys: List[Option[String]]): List[KeyWithIndex] = {

    val candidateRowIdxToQueryKey = tableMatching.keyMatches.flatten { keyMatch =>
      keyMatch.rowMatches.map(rowMatch => (rowMatch.candidateRowId, queryKeys(keyMatch.queryRowIdx).get.toLowerCase()))
    }.toMap

    val includeIdxes = candidateRowIdxToQueryKey.keys.toList

    tableKeys
      .zipWithIndex
      .flatMap {
        case (key, idx) if key.isDefined && includeIdxes.contains(idx) => Some(KeyWithIndex(candidateRowIdxToQueryKey(idx), idx))
        case _ => None
      }

  }

}
