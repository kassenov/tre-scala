package algorithms

import search.TableSearcher

trait Algorithm {

  def run(tableSearch: TableSearcher): List[List[String]]

}
