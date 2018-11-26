package top_spending_countries

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.{Calculator, InputProcessor, Runner, SchemaManager}

class TopNSpendingCountriesRDD(private val inputFiles: Array[String]) extends Calculator{
  val inputPurchases = inputFiles(0)
  val inputCountries = inputFiles(1)

  def calculateUsingRDD(hiveContext: HiveContext,  n: Int): Array[(String, BigDecimal)] = {
    val processor = new InputProcessor(hiveContext.sparkContext)
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

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    val array = calculateUsingRDD(hiveContext, n)
    new SchemaManager(hiveContext).createTopSpendingCountriesDF(array)
  }
}

object TopNSpendingCountriesRDD {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNSpendingCountriesRDD(args)
    Runner.run("spark_top_spending_countries_rdd", calculator, 10)
  }

}
