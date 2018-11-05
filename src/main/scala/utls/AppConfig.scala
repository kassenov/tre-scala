package utls

case class AppConfig(task: TaskFlow.Value,
                     queryRowsCount: Int,
                     columnsCount: Int,
                     columnsRelations: List[List[Int]],
                     concept: String,
                     docIds: List[Int])

object TaskFlow extends Enumeration {
  val Mapping, Evaluating, KeysAnalysis, Extracting = Value
}

object MapScoring extends Enumeration {
  val Simple, AdjacentMatchWeight = Value
}
