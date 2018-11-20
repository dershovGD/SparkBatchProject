package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.hive.HiveContext
import utils.InputProcessor

class TopNSpendingCountriesRDD(private val hiveContext: HiveContext) {
  private val processor = new InputProcessor(hiveContext.sparkContext)

  def calculateUsingRDD(inputPurchases: String, inputCountries: String, n: Int): Array[(String, BigDecimal)] = {
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

}
