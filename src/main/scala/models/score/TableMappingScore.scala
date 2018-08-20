package models.score

case class TableMappingScore(columns: List[Option[MappingScore]],
                             aggregatedByColumns: MappingScore,
                             rows: List[Option[MappingScore]],
                             aggregatedByRows: MappingScore,
                             total: MappingScore)
