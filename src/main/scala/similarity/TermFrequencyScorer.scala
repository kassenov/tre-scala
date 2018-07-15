package similarity

import statistics.TermFrequencyProvider

import scala.collection.mutable

class TermFrequencyScorer(termFrequencyProvider: TermFrequencyProvider) {

  private lazy val DEFAULT = java.lang.Double.MAX_VALUE

  private val weightCache = mutable.Map[String, Double]()

  /**
    * Returns term frequency fraction in the lucene index
    *
    * @param termString
    * @return
    */
  def get(termString: String): Double = {

    if (weightCache.contains(termString)) {
      weightCache(termString)
    } else {
      val weight = getTermFrequencyFraction(termString)
      weightCache.put(termString, weight)
      weight
    }

  }

  /**
    * The less frequent the term the higher its weight
    *
    * @param term
    * @return
    */
  private def getTermFrequencyFraction(term: String): Double = {

    val tf = termFrequencyProvider.get(term) match {
      case zeroTF if zeroTF == 0.0 => DEFAULT
      case nonZeroTF => nonZeroTF
    }

    1.0 / tf

  }

}
