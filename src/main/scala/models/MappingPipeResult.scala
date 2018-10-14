package models

import models.mapping.ColumnsMapping
import models.matching.{KeyWithIndex, TableMatch}

@SerialVersionUID(1000L)
case class MappingPipeResult(columnsMapping: ColumnsMapping,
                             candidateKeysWithIndexes: List[KeyWithIndex],
                             tableMatch: TableMatch)
