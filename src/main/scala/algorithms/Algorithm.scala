package algorithms

import search.TableSearch

trait Algorithm {

  def run(tableSearch: TableSearch): List[List[String]]

}
