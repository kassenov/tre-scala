package algorithms

import models.Table


trait Algorithm {
  def run(queryTable: Table): Table
}
