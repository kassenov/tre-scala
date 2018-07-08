//package models
//
//import java.io.{InputStream, InputStreamReader, Serializable}
//import java.nio.charset.Charset
//
//import com.google.common.base.Charsets
//import com.google.gson.Gson
//
//
//object RawTable {
//  protected val gson = new Gson
//  protected val utf8: Charset = Charsets.UTF_8
//
//  // InternetDomainName class
//  def fromJson(json: String): RawTable = gson.fromJson(json, classOf[RawTable])
//
//  def fromJson(jsonIn: InputStream): RawTable = gson.fromJson(new InputStreamReader(jsonIn, utf8), classOf[RawTable])
//}
//
//class RawTable() extends Serializable {
//  /* the actual relation extracted, always column oriented */
//  var relation: Array[Array[String]] = _
//  var pageTitle = "" // the <title> tag of the original page
//
//  var title = "" // content of a <caption> tag of the table, if it
//
//  // existed
//  var url = ""
//  var hasHeader: Boolean = null // true if the original HTML had <th> tags
//
//  var headerPosition: HeaderPosition.Value = _ // position of those th tags
//
//  var tableType: Nothing = null // table classification (entity,
//
//  // relational, matrix ...)
//  var termSet: Array[String] = null // top-terms extracted from the source page
//
//  // metadata used to identify and locate a table in the CC corpus
//  var tableNum: Int = -1 // index of the table in the list of tables on the
//
//  // original page
//  var s3Link = "" // link into S3
//
//  var recordEndOffset: Long = -1 // offsets into the CC file
//
//  var recordOffset: Long = -1
//  /*
//     * the following attributes are not set in the raw data, but are set by the
//     * example preprocessing step of the example indexer (see
//     * webreduce.tools.Indexer)
//     */ var columnTypes: Array[String] = null
//  var urlTermSet: Array[String] = null
//  var titleTermSet: Array[String] = null
//  var domain: String = null // extracted from the URL using Guava's
//
//  def getNumCols: Int = this.relation.length
//
//  def getAttributes: Array[String] = {
//    val attrs = new Array[String](getNumCols)
//    var i = 0
//    while ( {
//      i < getNumCols
//    }) {
//      attrs(i) = relation(i)(0)
//
//      {
//        i += 1; i - 1
//      }
//    }
//    attrs
//  }
//
//  def getRelation: Array[Array[String]] = relation
//
//  def setRelation(relation: Array[Array[String]]): Unit = {
//    this.relation = relation
//  }
//
//  def getTitle: String = title
//
//  def setTitle(title: String): Unit = {
//    this.title = title
//  }
//
//  def getUrl: String = url
//
//  def setUrl(url: String): Unit = {
//    this.url = url
//  }
//
//  def getHasHeader: Boolean = hasHeader
//
//  def setHasHeader(hasHeader: Boolean): Unit = {
//    this.hasHeader = hasHeader
//  }
//
//  def getS3Link: String = s3Link
//
//  def setS3Link(s3Link: String): Unit = {
//    this.s3Link = s3Link
//  }
//
//  def getTableNum: Int = tableNum
//
//  def setTableNum(tableNum: Int): Unit = {
//    this.tableNum = tableNum
//  }
//
//  def getRecordEndOffset: Long = recordEndOffset
//
//  def setRecordEndOffset(recordEndOffset: Long): Unit = {
//    this.recordEndOffset = recordEndOffset
//  }
//
//  def getRecordOffset: Long = recordOffset
//
//  def setRecordOffset(recordOffset: Long): Unit = {
//    this.recordOffset = recordOffset
//  }
//
//  def getColumnTypes: Array[String] = columnTypes
//
//  def setColumnTypes(columnTypes: Array[String]): Unit = {
//    this.columnTypes = columnTypes
//  }
//
//  def getDomain: String = domain
//
//  def setDomain(domain: String): Unit = {
//    this.domain = domain
//  }
//
//  def getTermSet: Array[String] = termSet
//
//  def setTermSet(termSet: Array[String]): Unit = {
//    this.termSet = termSet
//  }
//
//  def getUrlTermSet: Array[String] = urlTermSet
//
//  def setUrlTermSet(urlTermSet: Array[String]): Unit = {
//    this.urlTermSet = urlTermSet
//  }
//
//  def getTitleTermSet: Array[String] = titleTermSet
//
//  def setTitleTermSet(titleTermSet: Array[String]): Unit = {
//    this.titleTermSet = titleTermSet
//  }
//
//  def getHeaderPosition: HeaderPosition.Value = headerPosition
//
//  def setHeaderPosition(headerPosition: Nothing): Unit = {
//    this.headerPosition = headerPosition
//  }
//
//  def getPageTitle: String = pageTitle
//
//  def setPageTitle(pageTitle: String): Unit = {
//    this.pageTitle = pageTitle
//  }
//
//  def getTableType: Nothing = tableType
//
//  def setTableType(tableType: Nothing): Unit = {
//    this.tableType = tableType
//  }
//
//  def toJson: String = RawTable.gson.toJson(this)
//}
//
