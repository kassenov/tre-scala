package models

import models.mapping.ColumnsMapping
import models.matching.{KeyWithIndex, TableMatching}

case class MappingPipesResult(candidateTable: Table,
                              columnsMapping: ColumnsMapping,
                              candidateKeys: List[KeyWithIndex],
                              tableMatching: TableMatching)
