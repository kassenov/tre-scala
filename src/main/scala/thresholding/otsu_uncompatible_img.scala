package thresholding

object otsu_uncompatible_img {

  /**
    * Returns threshold based on Otsu's algorithm
    *
    * @param similarities list of similarities
    * @return
    */
  def getThreshold(similarities: List[Double]): Double = {
    calculate(similarities, similarities.length, similarities.sum)
  }

  // Similarity range from 0 to 100
  protected val RADIX = 100

  /**
    * Calculates threshold based on Otsu's algorithm
    * Based on https://github.com/JasonAltschuler/Otsu/blob/master/Otsu.java
    *
    * @param similarities list of similarities
    * @param n number of elements
    * @param sum sum of the similarities
    * @return
    */
  private def calculate(similarities: List[Double], n: Int, sum: Double): Double = {
     var threshold = .0

    var variance = .0
    // objective function to maximize
    var bestVariance = Double.NegativeInfinity
    var mean_bg = .0
    var weight_bg = .0
    var mean_fg = sum.toDouble / n.toDouble
    // mean of population
    var weight_fg = n.toDouble
    // weight of population
    var diff_means = .0
    // loop through all candidate thresholds
    var t = 0
    while ( {
      t < RADIX
    }) { // calculate variance
      diff_means = mean_fg - mean_bg
      variance = weight_bg * weight_fg * diff_means * diff_means
      // store best threshold
      if (variance > bestVariance) {
        bestVariance = variance
        threshold = t
      }
      // go to next candidate threshold
      while ( {
        t < RADIX && similarities(t) == 0
      }) {
        t += 1; t - 1
      }
      mean_bg = (mean_bg * weight_bg + similarities(t) * t) / (weight_bg + similarities(t))
      mean_fg = (mean_fg * weight_fg - similarities(t) * t) / (weight_fg - similarities(t))
      weight_bg += similarities(t)
      weight_fg -= similarities(t)
      t += 1
    }

    threshold

  }

}
