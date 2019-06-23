package algorithms

import java.util.concurrent.TimeUnit

import models.Table

import scala.collection.mutable


trait Timing {

  var startTimes: mutable.ListBuffer[Long] = _
  var levels: Int = _

  def initTimings(levels: Int): Unit = {
    startTimes = mutable.ListBuffer.fill(levels)(System.nanoTime)
    this.levels = levels
  }

  def reportWithDuration(level: Int, message: String, reset: Boolean = true): Unit = {
    val currentTime = System.nanoTime
    val duration = TimeUnit.NANOSECONDS.toSeconds(currentTime - startTimes(level))

    val levelIndent = "*" * level
    println(s"$levelIndent ${Console.RED}Timing:${Console.RESET} $message in ${Console.RED}$duration${Console.RESET} seconds $levelIndent")

    if (reset) {
      //resetting all nested timings
      List.range(level, this.levels).foreach(lvl => startTimes(lvl) = System.nanoTime)
    }
  }

  def run(queryTable: Table): Table
}
