package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.hive.HiveContext
import utils.InputProcessor

class TopNSpendingCountriesDF(private val hiveContext : HiveContext) {
  private val processor = new InputProcessor(hiveContext)
  def calculateUsingDF(inputPurchases: String, inputCountries: String, n: Int) : DataFrame = {
    val isInRange = udf((network :String , ip: String ) => new SubnetUtils(network).getInfo.isInRange(ip))
    val events = processor.
      createEventsDF(inputPurchases).
      cache()
    val countries = processor.
      createCountriesDF(inputCountries)
    events.join(countries, isInRange(countries("network"), events("ip_address"))).
      groupBy("country_name").
      sum("product_price").
      withColumnRenamed("sum(product_price)", "total_purchases").
      sort(desc("total_purchases")).
      limit(n)
  }

}
