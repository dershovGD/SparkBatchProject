package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.hive.HiveContext
import utils._

class TopNCategoriesDF(private val inputFiles: Array[String]) extends Calculator {
  val eventsFile = inputFiles(0)

  def calculateUsingDF(hiveContext: HiveContext, n: Int): DataFrame = {
    val schemaManager = new SchemaManager(hiveContext)
    schemaManager.createEventsDF(eventsFile).
      groupBy("category").
      count().
      sort(desc("count")).
      limit(n)
  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    calculateUsingDF(hiveContext, n)
  }
}

object TopNCategoriesDF {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNCategoriesDF(args)
    Runner.run("spark_top_categories_df", calculator, 10)
  }

}