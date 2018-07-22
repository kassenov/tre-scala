package models.matching

/**
  * Matching for a key query table key
  *
  * @param queryRowIdx row index of the query table
  * @param rowMatchings
  */
case class KeyMatching(queryRowIdx: Int, rowMatchings: List[RowMatching])
