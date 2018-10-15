package models.selection

case class KeyColumnsValuesModel(columnsCellValues: List[ColumnCellValues])

case class ColumnCellValues(clmnIdx: Int,
                            selectedValueIdx: Int,
                            score: Double,
                            values: List[CellValue])

case class CellValue(key: String,
                     relevance: Double,
                     coherence: Double,
                     docIds: Set[Int])
