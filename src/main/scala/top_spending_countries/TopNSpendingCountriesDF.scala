package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, desc, udf}
import org.apache.spark.sql.hive.HiveContext
import utils.{Calculator, Runner, SchemaManager}

class TopNSpendingCountriesDF(private val inputFiles: Array[String]) extends Calculator{
  val inputPurchases = inputFiles(0)
  val inputCountries = inputFiles(1)
  def   calculateUsingDF(hiveContext: HiveContext, n: Int): DataFrame = {
    val schemaManager = new SchemaManager(hiveContext)
    val isInRange = udf((network: String, ip: String) => new SubnetUtils(network).getInfo.isInRange(ip))
    val events = schemaManager.createEventsDF(inputPurchases) //n*logn
    val countries = schemaManager.createCountriesDF(inputCountries) //m*logm

    events.join(broadcast(countries), isInRange(countries("network"), events("ip_address"))). //n*m
      groupBy("country_name").
      sum("product_price").
      withColumnRenamed("sum(product_price)", "total_purchases").
      sort(desc("total_purchases")).
      limit(n)
      //(ipmin1, ipmax1) => (nmin1<ipmin1, nmax1> ipmax1)
      //countries broadcast
      //(ipmin, ipmax, country)

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