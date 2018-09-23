package models.relation

/**
  * Columns relation represents which columns inter-related.
  *
  * Examples:
  * [0,1,2] - Column 0, 1 and 2 are related (complex relation).
  * [0,3] - Column 0 and 3 are related (binary relation).
  *
  * @param linkedColumnIdxes
  */
case class TableColumnsRelation(linkedColumnIdxes: List[Int])
