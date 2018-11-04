package similarity

import scala.collection.parallel.CollectionConverters._

case class WeightedString(words: List[String], weights: List[Double], original: String, total: Double)

object WeightedString {

  def get(source: String, tfWeighter: TermFrequencyScorer): WeightedString = {

    val words = source.split(" ").toList
    val weights = words.par
      .filter(word => isAlpha(word))
      .map(word => tfWeighter.get(word))
      .toList

    new WeightedString(words, weights, source, weights.sum)

  }

  def isAlpha(name: String): Boolean = name.matches("[a-zA-Z]+")

}
