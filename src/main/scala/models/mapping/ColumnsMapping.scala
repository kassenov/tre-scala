package models.mapping

import models.score.TableMappingScore

/**
  * Candidate Table's mapping
  *
  * @param columnIdxes candidate table's column indexes per query table's column position
  * @param score
  */
case class ColumnsMapping(columnIdxes: List[Option[Int]], score: TableMappingScore)
