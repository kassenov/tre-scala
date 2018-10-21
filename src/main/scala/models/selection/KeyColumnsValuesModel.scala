package models.selection

case class KeyColumnsValuesModel(score: Double, columnsCellValues: List[ColumnCellValues])

case class ColumnCellValues(clmnIdx: Int,
                            selectedValue: Option[String],
                            values: List[CellValue])

case class CellValue(value: String,
                     relevance: Double,
                     coherence: Double,
                     score: Double, // What does it hold?
                     docIds: Set[Int])
