package top_products_by_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.{Calculator, Runner, SchemaManager}

class TopNProductsByCategoryDF(private val inputFiles: Array[String]) extends Calculator{
  val eventsFile = inputFiles(0)
  def calculateUsingDF(hiveContext: HiveContext, n: Int): DataFrame = {
    val windowSpec = Window.
      partitionBy("category").
      orderBy(desc("count"))

    val schemaManager = new SchemaManager(hiveContext)

    val dataFrame = schemaManager.createEventsDF(eventsFile).
      groupBy("category", "product_name").
      count().
      withColumn("row_number", row_number() over windowSpec)
    dataFrame.filter(dataFrame("row_number") < n)

  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    calculateUsingDF(hiveContext, n)
  }
}
object TopNProductsByCategoryDF {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNProductsByCategoryDF(args)
    Runner.run("spark_top_products_by_categories_df", calculator, 10)
  }

}
