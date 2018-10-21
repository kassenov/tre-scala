package utls

case class AppConfig(task: TaskFlow.Value, columnsCount: Int, columnsRelations: List[List[Int]], concept: String)

object TaskFlow extends Enumeration {
  val Mapping, Evaluating, KeysAnalysis = Value
}
