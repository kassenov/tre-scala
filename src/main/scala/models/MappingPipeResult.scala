package models

import models.mapping.ColumnsMapping
import models.matching.{KeyWithIndex, TableMatching}

case class MappingPipeResult(columnsMapping: ColumnsMapping,
                             candidateKeysWithIndexes: List[KeyWithIndex],
                             tableMatching: TableMatching)
