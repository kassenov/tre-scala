package models.mapping

import models.score.MappingScore

case class ColumnsMapping(queryToDestMap: List[Int], score: MappingScore)
