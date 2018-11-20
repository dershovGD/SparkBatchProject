package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.hive.HiveContext
import utils.SchemaManager

class TopNSpendingCountriesDF(private val hiveContext: HiveContext) {
  def calculateUsingDF(inputPurchases: String, inputCountries: String, n: Int): DataFrame = {
    val schemaManager = new SchemaManager(hiveContext)
    val isInRange = udf((network: String, ip: String) => new SubnetUtils(network).getInfo.isInRange(ip))
    val events = schemaManager.createEventsDF(inputPurchases).
      cache()
    val countries = schemaManager.createCountriesDF(inputCountries)
    events.join(countries, isInRange(countries("network"), events("ip_address"))).
      groupBy("country_name").
      sum("product_price").
      withColumnRenamed("sum(product_price)", "total_purchases").
      sort(desc("total_purchases")).
      limit(n)
  }

}
