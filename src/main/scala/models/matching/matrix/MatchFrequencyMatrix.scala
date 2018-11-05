package models.matching.matrix

/**
  * Match Frequency Matrix is a data structure that keeps number of matches of original table cell in another table.
  *
  * Match frequency matrix from original table (ot - query table) to destination table (dt):
  *
  *               ot.clmn-1   |   ot.clmn-2   | *** |   ot.clmn-n
  * ot.rec-1 ->      nm11     |      nm12     | *** |      nm1n
  * ot.rec-2 ->      nm21     |      nm22     | *** |      nm2n
  *   ***            ***      |      ***      | *** |      ***
  * ot.rec-m ->      nmm1     |      nmm2     | *** |      nmmn
  *
  * Each cell in the match matrix keeps number of matches where query key match key and query value match value.
  *
  * @param columns
  */
case class MatchFrequencyMatrix(columns: List[List[AdjacentMatches]])

case class AdjacentMatches(nPositive: Int, nPossible: Int)


