package models.mapping

import models.score.TableMappingScore

case class ColumnsMapping(columns: List[Option[Int]], score: TableMappingScore)
