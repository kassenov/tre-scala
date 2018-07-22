package models

import models.mapping.ColumnsMapping
import models.matching.KeyWithIndex

case class MappingPipesResult(candidateTable: Table,
                              columnsMapping: ColumnsMapping,
                              candidateKeys: List[KeyWithIndex])
