package top_categories

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.hive.HiveContext
import utils.{DBOutputWriter, InputProcessor}

class TopNCategoriesDF(private val hiveContext : HiveContext) {
  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    new InputProcessor(hiveContext).createEventsDF(inputFile).
      groupBy("category").
      count().
      sort(desc("count")).
      limit(n)
  }

}

object TopNCategoriesDF {
  def main(args : Array[String]) : Unit = {
    val dataFrame = calculate(args)
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/hadoop"
    val tableName = "spark_top_categories_df"
    new DBOutputWriter(prop, url, tableName).writeDataFrame(dataFrame)
  }

  def calculate(args : Array[String]) : DataFrame = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("TopNCategoriesDF")
    val sc = new SparkContext(conf)

    new TopNCategoriesDF(new HiveContext(sc)).calculateUsingDF(args(0), 10)
  }

}