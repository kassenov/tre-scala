package algorithms

import java.util.concurrent.TimeUnit

import models.Table
import models.relation.TableColumnsRelation
import search.TableSearcher

trait Algorithm {

  var startTime: Long = _

  def init(): Unit = {
    startTime = System.nanoTime
  }

  def reportDuration(reset: Boolean): Unit = {
    val currentTime = System.nanoTime
    val duration = TimeUnit.NANOSECONDS.toSeconds(currentTime - startTime)
    println(s"in $duration seconds")

    if (reset) {
      startTime = System.nanoTime
    }
  }

  def run(queryTable: Table, tableSearcher: TableSearcher, tableColumnsRelations: List[TableColumnsRelation]): Table
}
