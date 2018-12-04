package top_spending_countries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.{Logger, LoggerFactory}
import utils._

class TopNSpendingCountriesDF(private val inputFiles: Array[String]) extends Calculator {
  private val inputPurchases = inputFiles(0)
  private val inputCountries = inputFiles(1)
  private val slf4jLogger = LoggerFactory.getLogger("TopNSpendingCountriesDF")

  def calculateUsingDF(hiveContext: HiveContext, n: Int): DataFrame = {
    val inputProcessor = new InputProcessor(hiveContext.sparkContext)
    val schemaManager = new SchemaManager(hiveContext)
    val events = schemaManager.createEventsDF(inputPurchases)
    slf4jLogger.info("Events successfully read")

    val networkCountries = inputProcessor.
      readCountries(inputCountries)
    val countriesIpSorted = CountriesIpDataProcessor.processCountries(networkCountries)
    val finder = new CountryByIpFinder(countriesIpSorted)
    val finderBroadcast = hiveContext.sparkContext.broadcast(finder)
    slf4jLogger.info("Countries broadcast created")

    val isInRange = udf((ip: String) => {
      finderBroadcast.value.findCountryByIp(ip).orNull
    })

    val joinedData = events.withColumn("country_name", isInRange(events("ip_address")))
    slf4jLogger.info("Join successfully finished")
    joinedData.
      filter(col("country_name").isNotNull).
      groupBy("country_name").
      sum("product_price").
      withColumnRenamed("sum(product_price)", "total_purchases").
      sort(desc("total_purchases")).
      limit(n)
  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    calculateUsingDF(hiveContext, n)
  }
}

object TopNSpendingCountriesDF {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNSpendingCountriesDF(args)
    Runner.run("spark_top_spending_countries_df", calculator, 10)
  }

}