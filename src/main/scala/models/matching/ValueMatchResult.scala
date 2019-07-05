package models.matching

/**
  *
  * @param candidateIdx can be column (if value match) or row (if key match)
  * @param sim
  */
case class ValueMatchResult(candidateIdx: Int, sim: Double)
