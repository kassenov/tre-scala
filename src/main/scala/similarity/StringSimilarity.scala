package similarity

import org.apache.commons.text.similarity.LevenshteinDistance


trait StringSimilarity {

  def sim(query: String, in: String): Option[Double]

}

class ByWordLevenshteinSimilarity(tfWeighter: TermFrequencyScorer,
                                  threshold: Double = 0.8) {

  /**
    * Each query term is matched to a subject term where the levenshtein score is the highest.
    * The similarity weight is sum of normalized weight of those terms.
    *
    * @param query
    * @param in
    * @return
    */
  def sim(query: String, in: String): Option[Double] = {

    getSim(WeightedString.get(query, tfWeighter), WeightedString.get(in, tfWeighter))

  }

  private def getSim(query: WeightedString, in: WeightedString, threshold: Double = 0.8): Option[Double] = {

    val totalWeight = query.total + in.total

    query
      .words.par
      .zipWithIndex.map { case (queryWord, queryWordIdx) =>

        in.words.par
          .zipWithIndex
          .find { case (inWord, _) => getMatchScore(queryWord, inWord) > threshold }
          .map { case (_, inWordIdx) =>
            val normQueryWordWeight = query.weights(queryWordIdx) / totalWeight
            val normInWordWeight = in.weights(inWordIdx) / totalWeight
            normQueryWordWeight + normInWordWeight
          }

    }.sum match {
      case Some(score) if score > threshold => Some(score)
      case None                             => None
    }

  }

  private def getMatchScore(queryTerm: String, inTerm: String, minLengthToLevenshtein: Int = 4): Double = {
    val trimmedQueryTerm = queryTerm.trim
    val trimmedInTerm = inTerm.trim
    if (trimmedQueryTerm.length >= 4) {
      getLevenshteinScore(trimmedQueryTerm, trimmedInTerm)
    } else {
      getExactMatchScore(trimmedQueryTerm, trimmedInTerm)
    }
  }

  private lazy val levenshteinDistance = new LevenshteinDistance()

  /**
    * Returns matching fraction. Eg. (abcdef, abde) => 1 - (2 / max(6,4)) => 0.7
    *
    * @param queryTerm
    * @param inTerm
    * @return
    */
  private def getLevenshteinScore(queryTerm: String, inTerm: String): Double = {
    val distance = levenshteinDistance.apply(queryTerm, inTerm)
    1.0 - distance / math.max(queryTerm.length, inTerm.length)
  }

  private def getExactMatchScore(queryTerm: String, inTerm: String): Double = {
    if (queryTerm == inTerm) {
      1
    } else {
      0
    }
  }

}
