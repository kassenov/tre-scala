import java.io.File
import java.util.concurrent.TimeUnit

import algorithms.TrexAlgorithm
import models.Table
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import search.LuceneTableSearcher


object Main extends App {
  val rootPath = "/media/light/SSD/50"
  val concept = "countries"
//  val conceptKeys = List("Albania", "Greece", "Macedonia", "Serbia and Montenegro", "Andorra", "France", "Spain", "Austria", "Czech Republic", "Germany", "Hungary", "Italy", "Liechtenstein", "Slovakia", "Slovenia", "Switzerland", "Belarus", "Latvia", "Lithuania", "Poland", "Ukraine", "Russia", "Belgium", "Luxembourg", "Netherlands", "Bosnia and Herzegovina", "Croatia", "Bulgaria", "Romania", "Turkey", "Denmark", "Estonia", "Finland", "Norway", "Sweden", "Monaco", "Holy See", "Iceland", "Ireland", "San Marino", "Malta", "Moldova", "Portugal", "United Kingdom", "Afghanistan", "China", "Iran", "Pakistan", "Tajikistan", "Turkmenistan", "Uzbekistan", "Armenia", "Georgia", "Azerbaijan", "Bahrain", "Bangladesh", "Burma", "India", "Bhutan", "Brunei", "Malaysia", "Laos", "Thailand", "Cambodia", "Vietnam", "Kazakstan", "North Korea", "Kyrgyzstan", "Mongolia", "Nepal", "Cyprus", "Israel", "Egypt", "Indonesia", "Papua New Guinea", "Iraq", "Jordan", "Kuwait", "Saudi Arabia", "Syria", "Lebanon", "Japan", "South Korea", "Maldives", "Oman", "United Arab Emirates", "Yemen", "Philippines", "Qatar", "Singapore", "Sri Lanka", "Taiwan", "Antigua and Barbuda", "Bahamas", "Barbados", "Belize", "Guatemala", "Mexico", "Canada", "United States", "Costa Rica", "Nicaragua", "Panama", "Cuba", "Dominica", "Dominican Republic", "Haiti", "El Salvador", "Honduras", "Grenada", "Jamaica", "Colombia", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", "Trinidad and Tobago", "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia", "Nauru", "New Caledonia", "New Zealand", "Palau", "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu", "Western Samoa", "Argentina", "Bolivia", "Brazil", "Chile", "Paraguay", "Uruguay", "Peru", "French Guiana", "Guyana", "Suriname", "Venezuela", "Ecuador", "Algeria", "Libya", "Mali", "Mauritania", "Morocco", "Niger", "Tunisia", "Western Sahara", "Angola", "Congo", "Namibia", "Zaire", "Zambia", "Benin", "Burkina Faso", "Nigeria", "Togo", "Botswana", "South Africa", "Zimbabwe", "Cote dIvoire", "Ghana", "Burundi", "Rwanda", "Tanzania", "Cameroon", "Central African Republic", "Chad", "Equatorial Guinea", "Gabon", "Cape Verde", "Sudan", "Comoros", "Guinea", "Liberia", "Djibouti", "Eritrea", "Ethiopia", "Somalia", "Kenya", "Gambia", "Senegal", "Guinea-Bissau", "Sierra Leone", "Uganda", "Lesotho", "Madagascar", "Malawi", "Mozambique", "Mauritius", "Swaziland", "Sao Tome and Principe", "Seychelles")
//  val minMatchKeys = (conceptKeys.length * 0.05).toInt

  val sourceIndex = new SimpleFSDirectory(new File(rootPath + "/lucene-indexes/full-keys-to-raw-lidx").toPath)
  val reader = DirectoryReader.open(sourceIndex)
  val searcher = new IndexSearcher(reader)

  var tableSearch = new LuceneTableSearcher(searcher)

  val analyzer = new StandardAnalyzer
  val algorithm = new TrexAlgorithm(reader, analyzer)

  println("Start")
  val startTime = System.nanoTime

  val queryTableColumns = List(List("Russia"), List("Moscow"))
  val queryTable = new Table("Query", "None", keyIdx = Some(0), hdrIdx = None, columns = queryTableColumns)
  val result = algorithm.run(queryTable, tableSearch)

  val endTime = System.nanoTime
  val duration = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)
  println(s"Finished indexing for $concept. Total found ${result.length} in $duration seconds")

  val a = 10

}
