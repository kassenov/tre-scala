package algorithms

import models.Table
import search.TableSearcher

trait Algorithm {

  def run(queryTable: Table, tableSearcher: TableSearcher): List[List[String]]

}
