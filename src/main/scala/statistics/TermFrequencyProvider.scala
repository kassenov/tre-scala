package statistics

import models.index.IndexFields
import org.apache.lucene.index.{IndexReader, Term}

trait TermFrequencyProvider {

  def get(term: String): Double

}

class LuceneIndexTermFrequencyProvider(indexReader: IndexReader, field: IndexFields.Value) extends TermFrequencyProvider {

  override def get(term: String): Double = {
    indexReader.totalTermFreq(new Term(field.toString, term)).toDouble
  }

}
