package utls

import java.io.File
import java.nio.charset.StandardCharsets

import de.siegmar.fastcsv.reader.CsvReader
import de.siegmar.fastcsv.writer.CsvWriter
import models.Table

import scala.util.{Failure, Success, Try}

class CsvUtils() {

  def exportTable(table: Table, name: String): Unit = {
    val file = new File(s"outputs/$name.csv")

    if (file.exists()) {
      file.delete()
    }

    val csvWriter = new CsvWriter()

    val columnsCount = table.columns.length
    val rowsCount = table.columns.head.length

    val csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)
    try {
      var startFrom = 0
      // header
      val clmnHdrs =
        if (table.hdrIdx.isDefined) {
          startFrom = table.hdrIdx.get + 1
          table.columns.flatMap(c => c(table.hdrIdx.get))
        } else {
          List.range(0, columnsCount).map(i => s"header${i+1}")
        }
      csvAppender.appendLine(clmnHdrs.toArray: _*)

      // records
      List.range(startFrom, rowsCount).foreach { rowIdx =>
        val recordValues = table.columns.flatMap(c => c(rowIdx))
        csvAppender.appendLine(recordValues.toArray: _*)
      }

      csvAppender.endLine()
    } finally if (csvAppender != null) csvAppender.close()
  }

  def importTable(name: String, clmnsCount: Int, hdrRowIdx: Option[Int]): Table = {
    val file = new File(s"outputs/$name.csv")
    val csvReader = new CsvReader()

    val csvParser = csvReader.parse(file, StandardCharsets.UTF_8)
    val records = Iterator.continually(csvParser.nextRow()).takeWhile(_ != null) map { row =>
      List.range(0, clmnsCount).map { clmnIdx =>
        val field = Try(row.getField(clmnIdx)) match {
          case Success(f) => f
          case Failure(ex) => ""
        }
        Some(field)
      }
    }

    Table(
      docId = -1,
      title = name,
      url = "no",
      keyIdx = Some(0),
      hdrIdx = hdrRowIdx,
      columns = records.toList.transpose
    )

  }

}
