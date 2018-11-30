package top_spending_countries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import utils._

class TopNSpendingCountriesDF(private val inputFiles: Array[String]) extends Calculator{
  val inputPurchases = inputFiles(0)
  val inputCountries = inputFiles(1)
  def   calculateUsingDF(hiveContext: HiveContext, n: Int): DataFrame = {
    val schemaManager = new SchemaManager(hiveContext)

    val events = schemaManager.createEventsDF(inputPurchases)

    val processor = new InputProcessor(hiveContext.sparkContext)
    val networkCountries = processor.
      readCountries(inputCountries).
      map(country => NetworkCountry(country.network, country.countryName))
    val countriesIpBroadcasted = hiveContext.sparkContext.broadcast(networkCountries)
    val isInRange = udf((ip: String) => {
      new CountryByIpFinder(countriesIpBroadcasted.value).findCountryByIp(ip).orNull
    })

    events.withColumn("country_name", isInRange(events("ip_address"))).
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