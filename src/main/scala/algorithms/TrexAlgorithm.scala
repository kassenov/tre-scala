package algorithms

import models.Table
import search.TableSearch
import transformers.Transformer

class TrexAlgorithm extends Algorithm {

  val transformer = new Transformer

  var usedTables: List[Table] = _

  override def run(queryTable: Table, tableSearch: TableSearch): List[List[String]] = {

    tableSearch.getRawJsonTablesByKeys(Table.getKeys(queryTable)).par
      .map(jsonTable => transformer.rawJsonToTable(jsonTable))
      .map { candidateTable =>

      }

  }

}
