package algorithms

import models.Table
import models.relation.TableColumnsRelation
import search.TableSearcher

trait Algorithm {

  def run(queryTable: Table, tableSearcher: TableSearcher, tableColumnsRelations: List[TableColumnsRelation]): Table

}
