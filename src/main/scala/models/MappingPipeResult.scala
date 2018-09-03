package models

import models.mapping.ColumnsMapping
import models.matching.{KeyWithIndex, TableMatches}

case class MappingPipeResult(columnsMapping: ColumnsMapping,
                             candidateKeysWithIndexes: List[KeyWithIndex],
                             tableMatching: TableMatches)
