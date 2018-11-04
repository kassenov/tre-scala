package similarity

import org.apache.commons.text.similarity.LevenshteinDistance
import scala.collection.parallel.CollectionConverters._


trait StringSimilarity {

  def sim(query: String, in: String): Double

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
  def sim(query: String, in: String): Double = {

    getSim(query, in)

  }

  def isAlphaNumeric(name: String): Boolean = name.matches("[\\d\\w]+")

  private def getSim(query: String, in: String, threshold: Double = 0.8): Double = {

    val queryWords = query.split(" ").toList
    val inWords = in.split(" ").toList

    val bestMatchScoresWithInWordPerQueryWord =
      queryWords.par
        .filter(word => isAlphaNumeric(word)) //FIXME do we need alphanumeric only?
        .flatMap { queryWord =>

          val matchScoreWithInWords = inWords.par
            .filter(word => isAlphaNumeric(word))
            .map { inWord =>
              getMatchScore(queryWord, inWord)
            }

          if (matchScoreWithInWords.isEmpty) {
            None
          } else {
            Some(matchScoreWithInWords.max)
          }

        }.toList

    val sim = bestMatchScoresWithInWordPerQueryWord.sum / math.max(queryWords.length, inWords.length)

    if (sim >= threshold) {
      sim
    } else {
      0.0
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

  // TODO weighted similarity
  private def getWeightedSim(query: WeightedString, in: WeightedString, threshold: Double = 0.8): Option[Double] = {

    val totalWeight = query.total + in.total

    val perWordWeights = query
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

    }.toList.flatten

    if (perWordWeights.isEmpty) {
      None
    } else {
      perWordWeights.sum match {
        case score if score > threshold => Some(score)
        case _                          => None
      }
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
    1.0 - distance.toDouble / math.max(queryTerm.length, inTerm.length).toDouble
  }

  private def getExactMatchScore(queryTerm: String, inTerm: String): Double = {
    if (queryTerm == inTerm) {
      1d
    } else {
      0d
    }
  }

}
