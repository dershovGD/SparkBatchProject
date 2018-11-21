package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.{Calculator, Runner, SchemaManager}

class TopNSpendingCountriesDF(private val hiveContext: HiveContext) extends Calculator{
  def   calculateUsingDF(inputPurchases: String, inputCountries: String, n: Int): DataFrame = {
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

  override def calculate(args: Array[String], n: Int): DataFrame = {
    calculateUsingDF(args(0), args(1), n)
  }
}
object TopNSpendingCountriesDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("TopNSpendingCountriesDF")
      .set("spark.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val sc = new SparkContext(conf)
    val calculator = new TopNSpendingCountriesDF(new HiveContext(sc))

    Runner.run("spark_top_spending_countries_df", calculator, args, 10)
  }

}