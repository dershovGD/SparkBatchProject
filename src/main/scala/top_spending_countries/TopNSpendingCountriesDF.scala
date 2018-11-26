package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.{Calculator, Runner, SchemaManager}

class TopNSpendingCountriesDF(private val inputFiles: Array[String]) extends Calculator{
  val inputPurchases = inputFiles(0)
  val inputCountries = inputFiles(1)
  def   calculateUsingDF(hiveContext: HiveContext, n: Int): DataFrame = {
    val schemaManager = new SchemaManager(hiveContext)
    val isInRange = udf((network: String, ip: String) => new SubnetUtils(network).getInfo.isInRange(ip))
    val events = schemaManager.createEventsDF(inputPurchases).
      cache()
    val countries = schemaManager.createCountriesDF(inputCountries).cache()
    events.join(countries, isInRange(countries("network"), events("ip_address"))).
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