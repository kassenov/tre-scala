import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit

import indexing.KeysIndexer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.Version
import reader.ArchiveReader

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


object Main extends App {
  val concept = "countries"
  val conceptKeys = List("Albania", "Greece", "Macedonia", "Serbia and Montenegro", "Andorra", "France", "Spain", "Austria", "Czech Republic", "Germany", "Hungary", "Italy", "Liechtenstein", "Slovakia", "Slovenia", "Switzerland", "Belarus", "Latvia", "Lithuania", "Poland", "Ukraine", "Russia", "Belgium", "Luxembourg", "Netherlands", "Bosnia and Herzegovina", "Croatia", "Bulgaria", "Romania", "Turkey", "Denmark", "Estonia", "Finland", "Norway", "Sweden", "Monaco", "Holy See", "Iceland", "Ireland", "San Marino", "Malta", "Moldova", "Portugal", "United Kingdom", "Afghanistan", "China", "Iran", "Pakistan", "Tajikistan", "Turkmenistan", "Uzbekistan", "Armenia", "Georgia", "Azerbaijan", "Bahrain", "Bangladesh", "Burma", "India", "Bhutan", "Brunei", "Malaysia", "Laos", "Thailand", "Cambodia", "Vietnam", "Kazakstan", "North Korea", "Kyrgyzstan", "Mongolia", "Nepal", "Cyprus", "Israel", "Egypt", "Indonesia", "Papua New Guinea", "Iraq", "Jordan", "Kuwait", "Saudi Arabia", "Syria", "Lebanon", "Japan", "South Korea", "Maldives", "Oman", "United Arab Emirates", "Yemen", "Philippines", "Qatar", "Singapore", "Sri Lanka", "Taiwan", "Antigua and Barbuda", "Bahamas", "Barbados", "Belize", "Guatemala", "Mexico", "Canada", "United States", "Costa Rica", "Nicaragua", "Panama", "Cuba", "Dominica", "Dominican Republic", "Haiti", "El Salvador", "Honduras", "Grenada", "Jamaica", "Colombia", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", "Trinidad and Tobago", "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia", "Nauru", "New Caledonia", "New Zealand", "Palau", "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu", "Western Samoa", "Argentina", "Bolivia", "Brazil", "Chile", "Paraguay", "Uruguay", "Peru", "French Guiana", "Guyana", "Suriname", "Venezuela", "Ecuador", "Algeria", "Libya", "Mali", "Mauritania", "Morocco", "Niger", "Tunisia", "Western Sahara", "Angola", "Congo", "Namibia", "Zaire", "Zambia", "Benin", "Burkina Faso", "Nigeria", "Togo", "Botswana", "South Africa", "Zimbabwe", "Cote dIvoire", "Ghana", "Burundi", "Rwanda", "Tanzania", "Cameroon", "Central African Republic", "Chad", "Equatorial Guinea", "Gabon", "Cape Verde", "Sudan", "Comoros", "Guinea", "Liberia", "Djibouti", "Eritrea", "Ethiopia", "Somalia", "Kenya", "Gambia", "Senegal", "Guinea-Bissau", "Sierra Leone", "Uganda", "Lesotho", "Madagascar", "Malawi", "Mozambique", "Mauritius", "Swaziland", "Sao Tome and Principe", "Seychelles")
  val minMatchKeys = (conceptKeys.length * 0.05).toInt

  val rootPath = "/media/light/SSD/50"
  val folderPath = s"$rootPath/filtered/$concept/"

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)

  val searcher = new IndexSearcher(reader)
  val analyzer = new StandardAnalyzer

  val field = "keys"
  val parser = new QueryParser(field, analyzer)

  val builder = new BooleanQuery.Builder()

  conceptKeys.foreach { key =>
    val parsedKey = parser.parse(key)
    builder.add(parsedKey, BooleanClause.Occur.SHOULD)
  }

  val query = builder.build()

  System.out.println("Searching for: " + query.toString(field))

  val initialSearchResult = searcher.search(query, 1)

  searcher
    .search(query, initialSearchResult.totalHits.toInt).scoreDocs
    .foreach { scoreDoc =>
      if (searcher.explain(query, scoreDoc.doc).getDetails.length > keys.length * .07) {
        val document = searcher.doc(scoreDoc.doc)
        val name = document.get("name").split("/")(1)
        val raw = document.get("raw")
        val pw = new PrintWriter(new File(s"$folderPath/$name"), "UTF-8")
        pw.write(raw)
        pw.close
      }
    }

//  searcher
//    .search(query, initialSearchResult.totalHits.toInt).scoreDocs
//    .flatMap { scoreDoc =>
//      if (searcher.explain(query, scoreDoc.doc).getDetails.length > keys.length * .2) {
//        Some(scoreDoc)
//      } else {
//        None
//      }
//    }
//    .foreach { scoreDoc =>
//      val document = searcher.doc(scoreDoc.doc)
//      val name = document.get("name").split("/")(1)
//      val raw = document.get("raw")
//      val pw = new PrintWriter(new File(s"$rootPath/countries/$name"), "UTF-8")
//      pw.write(raw)
//      pw.close
//    }

  val a = 10

}

//def indexing(): Unit = {
//
//  val archiveReader = new ArchiveReader
//  val indexer = new KeysIndexer
//
//  println("Start")
//  val rootPath = "/media/light/SSD/50/50-tar"
//  val folders = List(50)
//
//  folders.foreach { folder =>
//
//    println(s"Start indexing group $folder")
//    val startTime = System.nanoTime
//
//    val files = archiveReader.getArchivedFiles(s"$rootPath/$folder.tar")
//
//    val analyzer = new EnglishAnalyzer()
//
//    val idx_dir = new SimpleFSDirectory(new File(rootPath + "/lidx").toPath)
//    val indexConfig = new IndexWriterConfig(analyzer)
//    val writer = new IndexWriter(idx_dir, indexConfig)
//
//    files.par.foreach { file =>
//      val fileName = file._1
//      println(file._1)
//      val rawTable = file._2
//      indexer.index(fileName, rawTable, writer, analyzer)
//    }
//
//    writer.close
//
//    val endTime = System.nanoTime
//    val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
//    println(s"Finished indexing group $folder in $duration seconds")
//  }
//
//  println("Finish")
//
//}
