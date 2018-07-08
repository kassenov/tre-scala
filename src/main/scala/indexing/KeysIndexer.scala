package indexing

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter

class KeysIndexer {

  def index(name: String, rawTable: String, writer: IndexWriter, analyzer: EnglishAnalyzer): Unit = {

    val jsonTable = ujson.read(rawTable)

    val url = jsonTable("url").str
    if (!(url.contains(".com") || url.contains(".net") || url.contains(".org") || url.contains(".uk"))) return

    val tableType = jsonTable("tableType").str
    val tblOrientation = jsonTable("tableOrientation").str
    val relation = jsonTable("relation")

    val hdrRowIdx = jsonTable("headerRowIndex").num
    val keyClnIdx = jsonTable("keyColumnIndex").num

    val tblHasHdr = jsonTable("hasHeader").bool
    val tblHasKey = jsonTable("hasKeyColumn").bool

    val buff_cols = relation.arr map(_.arr.map(el => el.str).toList)
    val columns = if (tblOrientation == "HORIZONTAL") {
      buff_cols.toList
    } else {
      buff_cols.toList.transpose
    }

    val keysStr = columns.head.mkString(" ")

    val doc = new Document
    doc.add(new StoredField("name", name))
    doc.add(new TextField("keys", keysStr, Field.Store.NO))
    doc.add(new StoredField("raw", rawTable))

    writer.addDocument(doc)

  }

}
