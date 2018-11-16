import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

class TopNSpendingCountries(private val hiveContext : HiveContext) {
  private val processor = new InputOutputProcessor(hiveContext)
  def calculateUsingRDD(inputPurchases: String, inputCountries: String, n: Int) : Array[(String, BigDecimal)] = {
    val purchases = processor.readFromFile(inputPurchases).
      map(line => (line(4), BigDecimal(line(1)))).
      reduceByKey(_ + _).
      cache()
    val countries_ip = processor.readFromFile(inputCountries).
      map(line => (line(0), line(2))).
      cache()

    purchases.cartesian(countries_ip).
      filter(record => new SubnetUtils(record._2._1).getInfo.isInRange(record._1._1)).
      map(record => (record._2._2, record._1._2)).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(n)
  }

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
