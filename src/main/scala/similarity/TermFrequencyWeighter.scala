package similarity

import models.index.IndexFields
import org.apache.lucene.index.{IndexReader, Term}

import scala.collection.mutable

class TermFrequencyWeighter(indexReader: IndexReader, field: IndexFields.Value) {

  private lazy val DEFAULT = java.lang.Double.MAX_VALUE

  private val weightCache = mutable.Map[String, Double]()

  def weight(termString: String): Double = {

    if (weightCache.contains(termString)) {
      weightCache(termString)
    } else {
      val weight = getWeight(termString)
      weightCache.put(termString, weight)
      weight
    }

  }

  /**
    * The less frequent the term the higher its weight
    *
    * @param termString
    * @return
    */
  private def getWeight(termString: String): Double = {
    val tf = termFrequency(termString) match {
      case zeroTF if zeroTF == 0.0 => DEFAULT
      case nonZeroTF => nonZeroTF
    }

    1.0 / tf
  }

  private val tfCache = mutable.Map[String, Double]()

  private def termFrequency(termString: String): Double = {
    indexReader.totalTermFreq(new Term(field.toString, termString)).toDouble
  }

}
