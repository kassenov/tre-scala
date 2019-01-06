package utls

case class AppConfig(task: TaskFlow.Value,
                     queryRowsCount: Int,
                     columnsCount: Int,
                     columnsRelations: List[List[Int]],
                     maxK: Int,
                     concept: String,
                     docIds: List[Int],
                     scoringMethod: MapScoring.Value,
                     extParams: List[String])

object TaskFlow extends Enumeration {
  val Mapping, Evaluating, KeysAnalysis, Extracting, ExtEvalPairWise = Value
}

object MapScoring extends Enumeration {
  val Simple, AdjacentMatchWeight = Value
}
