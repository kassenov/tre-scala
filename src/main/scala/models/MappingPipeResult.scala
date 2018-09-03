package models

import models.mapping.ColumnsMapping
import models.matching.{KeyWithIndex, TableMatch}

case class MappingPipeResult(columnsMapping: ColumnsMapping,
                             candidateKeysWithIndexes: List[KeyWithIndex],
                             tableMatching: TableMatch)
