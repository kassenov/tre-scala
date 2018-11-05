package utls

case class AppConfig(task: TaskFlow.Value,
                     queryRowsCount: Int,
                     columnsCount: Int,
                     columnsRelations: List[List[Int]],
                     concept: String,
                     docIds: List[Int],
                     scoringMethod: MapScoring.Value)

object TaskFlow extends Enumeration {
  val Mapping, Evaluating, KeysAnalysis, Extracting = Value
}

object MapScoring extends Enumeration {
  val Simple, AdjacentMatchWeight = Value
}
