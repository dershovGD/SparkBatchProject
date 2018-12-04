package top_spending_countries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import utils._

class TopNSpendingCountriesRDD(private val inputFiles: Array[String]) extends Calculator {
  val inputPurchases = inputFiles(0)
  val inputCountries = inputFiles(1)
  private val slf4jLogger = LoggerFactory.getLogger("TopNSpendingCountriesRDD")

  def calculateUsingRDD(hiveContext: HiveContext, n: Int): Array[(String, BigDecimal)] = {
    val inputProcessor = new InputProcessor(hiveContext.sparkContext)
    val purchases = inputProcessor.readEvents(inputPurchases).
      map(event => (event.ipAddress, event.productPrice)).
      reduceByKey(_ + _).
      map(r => TotalPurchasesIp(r._1, r._2))
    slf4jLogger.info("Events successfully read")

    val networkCountries = inputProcessor.
      readCountries(inputCountries)
    val countriesIpSorted = CountriesIpDataProcessor.processCountries(networkCountries)
    val finder = new CountryByIpFinder(countriesIpSorted)
    val finderBroadcast = hiveContext.sparkContext.broadcast(finder)
    slf4jLogger.info("Countries broadcast created")

    val joinedData = purchases.map(entry => TotalPurchasesCountry(
      finderBroadcast.value.findCountryByIp(entry.ipAddress).orNull,
      entry.totalPurchases))
    slf4jLogger.info("Join successfully finished")
    joinedData.
      filter(r => r.country != null).
      keyBy(_.country).
      mapValues(_.totalPurchases).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(n)
  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    val array = calculateUsingRDD(hiveContext, n)
    new SchemaManager(hiveContext).createTopSpendingCountriesDF(array)
  }
}

case class TotalPurchasesIp(ipAddress: String, totalPurchases: BigDecimal)

case class TotalPurchasesCountry(country: String, totalPurchases: BigDecimal)

object TopNSpendingCountriesRDD {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNSpendingCountriesRDD(args)
    Runner.run("spark_top_spending_countries_rdd", calculator, 10)
  }

}
