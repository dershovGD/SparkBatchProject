package top_products_by_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.{Calculator, Runner, SchemaManager}

class TopNProductsByCategoryDF(private val hiveContext: HiveContext) extends Calculator{
  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    val windowSpec = Window.
      partitionBy("category").
      orderBy(desc("count"))

    val schemaManager = new SchemaManager(hiveContext)

    val dataFrame = schemaManager.createEventsDF(inputFile).
      groupBy("category", "product_name").
      count().
      withColumn("row_number", row_number() over windowSpec).
      cache()
    dataFrame.filter(dataFrame("row_number") < n)

  }

  override def calculate(args: Array[String], n: Int): DataFrame = {
    calculateUsingDF(args(0), n)
  }
}
object TopNProductsByCategoryDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("TopNProductsByCategoryDF")
      .set("spark.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val sc = new SparkContext(conf)
    val calculator = new TopNProductsByCategoryDF(new HiveContext(sc))

    Runner.run("spark_top_products_by_categories_df", calculator, args, 10)
  }

}
