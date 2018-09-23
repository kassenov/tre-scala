package models.matching

case class RowMatch(candidateRowId: Int, queryClmIdxCellMatchMap: Map[Int, CellMatch])
