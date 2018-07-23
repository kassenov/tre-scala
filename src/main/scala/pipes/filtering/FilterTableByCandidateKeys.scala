package pipes.filtering

import models.matching.KeyWithIndex

class FilterTableByCandidateKeys() {

  def apply(candidateKeys: List[KeyWithIndex]): Boolean = {
    candidateKeys.nonEmpty
  }

}
