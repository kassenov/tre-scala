package thresholding

import statistics.StatUtils

object otsu {

  /**
    * Returns threshold based on Otsu's algorithm
    *
    * @param similarities list of similarities
    * @return
    */
  def getThreshold(similarities: List[Double]): Double = {
    calculate(similarities, similarities.length, similarities.sum, step=0.1)
  }


  /**
    * Calculates threshold based on Otsu's algorithm
    * Note: It uses highest threshold that has minimal mixed variance
    *
    * @param values list of values
    * @param n number of elements
    * @param sum sum of the similarities
    * @return
    */
  private def calculate(values: List[Double], n: Int, sum: Double, step: Double): Double = {

    // Objective function to minimize mixed variance
    var bestMixedVariance = Double.PositiveInfinity

    val max = values.max
    val min = values.min

    var bestThreshold = max

    (min to max by step).foreach { threshold =>

      val valuesUnder = values.filter(x => x <= threshold)
      val weightUnder = valuesUnder.size / values.size
      val varianceUnder = StatUtils.variance1(valuesUnder)

      val valuesOver = values.filter(x => x > threshold)
      val weightOver = valuesOver.size / values.size
      val varianceOver = StatUtils.variance1(valuesOver)

      val mixedVariance = weightUnder * varianceUnder + weightOver * varianceOver
      if (mixedVariance <= bestMixedVariance) {
        bestMixedVariance = mixedVariance
        bestThreshold = threshold
      }

    }

    bestThreshold

  }

}
