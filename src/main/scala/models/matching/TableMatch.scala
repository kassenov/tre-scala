package models.matching

/**
  * Each query row can have multiple row matches
  *
  * @param keyMatches
  */
case class TableMatch(keyMatches: List[QueryKeyToRowMatches])
