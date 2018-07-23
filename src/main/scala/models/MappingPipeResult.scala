package models

import models.mapping.ColumnsMapping
import models.matching.{KeyWithIndex, TableMatching}

case class MappingPipeResult(candidateTable: Table,
                             columnsMapping: ColumnsMapping,
                             candidateKeys: List[KeyWithIndex],
                             tableMatching: TableMatching)
