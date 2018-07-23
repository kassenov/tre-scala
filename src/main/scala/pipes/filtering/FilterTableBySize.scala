package pipes.filtering

import models.Table

class FilterTableBySize(minRows: Int, minCols: Int) {

  def apply(table: Table): Boolean = {
    table.columns.nonEmpty && table.columns.head.nonEmpty &&
      table.columns.size >= minCols && table.columns.head.size >= minRows
  }

}
