package similarity

import org.apache.commons.text.similarity.LevenshteinDistance


trait StringSimilarity {

  def sim(query: String, in: String): Option[Double]

}

class ByWordLevenshteinSimilarity(tfWeighter: TermFrequencyWeighter,
                                  threshold: Double = 0.8) {

  def sim(query: String, in: String): Option[Double] = {

    getSim(WeightedString.get(query, tfWeighter), WeightedString.get(in, tfWeighter))

  }

  private def getSim(query: WeightedString, in: WeightedString): Option[Double] = {

    val totalWeight = query.total + in.total

    val result =
      query
        .words.par
        .zipWithIndex.map { case (queryWord, queryWordIdx) =>

          in.words.par
            .zipWithIndex
            .find { case (inWord, _) => getLeveshteinScore(queryWord, inWord) > threshold }
            .map { case (_, inWordidx) =>
              val normQueryWordWeight = query.weights(queryWordIdx) / totalWeight
              val normInWordWeight = in.weights(inWordidx) / totalWeight
              normQueryWordWeight + normInWordWeight
            }

      }.sum

    result

  }

  private lazy val levenshteinDistance = new LevenshteinDistance()

  private def getLeveshteinScore(queryTerm: String, inTerm: String): Double = {
    var distance = levenshteinDistance.apply(queryTerm, inTerm)
    1.0 - distance / math.max(queryTerm.length, inTerm.length)
  }

}
