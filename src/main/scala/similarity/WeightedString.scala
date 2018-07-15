package similarity

case class WeightedString(words: List[String], weights: List[Double], original: String, total: Double)

object WeightedString {

  def get(source: String, tfWeighter: TermFrequencyWeighter): WeightedString = {

    val words = source.split(" ").toList
    val weights = words
      .map(word => tfWeighter.weight(word))

    new WeightedString(words, weights, source, weights.sum)

  }

}