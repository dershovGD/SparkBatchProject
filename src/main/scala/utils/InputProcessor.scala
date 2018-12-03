package utils

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class InputProcessor(private val sc: SparkContext) {

  def readEvents(inputFile: String): RDD[Event] = {
    val separator = ","

    sc.textFile(inputFile)
      .map(line => line.split(separator))
      .map(Event)
  }

  def readCountries(inputFile: String): Array[Country] = {
    val separator = ","

    val lines = sc.textFile(inputFile)
    lines.map(line => Country(line.split(separator))).
      collect()
  }

}

case class Event(lines: Array[String]) {
  val productName = lines(0)
  val productPrice = BigDecimal(lines(1))
  val purchaseDate = Timestamp.valueOf(lines(2))
  val category = lines(3)
  val ipAddress = lines(4)
}

case class Country(lines: Array[String]) {
  val network = lines(0)
  val countryIsoCode = lines(1)
  val countryName = lines(2)
}
