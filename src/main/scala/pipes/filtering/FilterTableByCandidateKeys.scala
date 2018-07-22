package pipes.filtering

import models.matching.KeyWithIndex

class FilterTableByCandidateKeys() {

  def filter(candidateKeys: List[KeyWithIndex]): Boolean = {
    candidateKeys.nonEmpty
  }

}
