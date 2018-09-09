package statistics

object StatUtils {

  def mean(X: List[Double]): Double = {
    if (X.isEmpty) {
      .0 // FIXME Should be undefined? https://gist.github.com/softprops/3936429
    } else {
      X.sum / X.size
    }
  }

  /**
    * σ
    *
    * @param X
    * @return
    */
  def standardDeviation(X: List[Double]): Double = {
    if (X.isEmpty) {
      .0 // FIXME Should be undefined? https://gist.github.com/softprops/3936429
    } else {

      Math.sqrt(variance1(X))
    }
  }

  /**
    * σ2
    *
    * @param X
    * @return
    */
  def variance1(X: List[Double]): Double = {
    val μ = mean(X)
    X.map( _ - μ ).map(t => t * t ).sum / X.size
  }

  /**
    * σ2
    * A bit more efficient way to calculate https://www.sciencebuddies.org/science-fair-projects/science-fair/variance-and-standard-deviation
    *
    * @param X
    * @return
    */
  def variance2(X: List[Double]): Double = {
    val μ = mean(X)
    (X.map(t => Math.pow(t, 2)).sum / X.size) - Math.pow(μ, 2)
  }

}
