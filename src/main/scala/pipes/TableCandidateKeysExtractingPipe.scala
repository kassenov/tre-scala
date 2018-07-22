package pipes

import models.Table
import models.matching.TableMatching

class TableCandidateKeysExtractingPipe() {

  def process(table: Table, tableMatching: TableMatching): List[String] = {

    tableMatching.keyMatches.map { keyMatch =>

    }

  }

}
