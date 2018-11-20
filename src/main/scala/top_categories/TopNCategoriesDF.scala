package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.{DBOutputWriter, InputProcessor, SchemaManager}

class TopNCategoriesDF(private val hiveContext: HiveContext) {
  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    val inputProcessor = new InputProcessor(hiveContext.sparkContext)
    val schemaManager = new SchemaManager(hiveContext, inputProcessor)
    schemaManager.createEventsDF(inputFile).
      groupBy("category").
      count().
      sort(desc("count")).
      limit(n)
  }

}

object TopNCategoriesDF {
  def main(args: Array[String]): Unit = {
    val dataFrame = calculate(args)
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/hadoop"
    val tableName = "spark_top_categories_df"
    new DBOutputWriter(prop, url, tableName).writeDataFrame(dataFrame)
  }

  def calculate(args: Array[String]): DataFrame = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("TopNCategoriesDF")
      .set("spark.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val sc = new SparkContext(conf)
    sc.addFile("file:/home/cloudera/tmp/events/18/10/31", recursive = true)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    new TopNCategoriesDF(new HiveContext(sc)).calculateUsingDF(args(0), 10)
  }

}