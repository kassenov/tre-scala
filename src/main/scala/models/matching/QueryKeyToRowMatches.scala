package models.matching

/**
  * Matching for a key query table key
  *
  * @param queryRowIdx row index of the query table
  * @param rowMatches
  */
case class QueryKeyToRowMatches(queryRowIdx: Int, rowMatches: List[RowMatch])
