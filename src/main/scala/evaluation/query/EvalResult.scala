package evaluation.query

case class EvalResult(queryKeys: List[Option[String]], clmnIdxToNToCount: Map[Int, Map[Int, Int]], clmnIdxToNToEntropy: Option[Map[Int, Map[Int, Double]]])
