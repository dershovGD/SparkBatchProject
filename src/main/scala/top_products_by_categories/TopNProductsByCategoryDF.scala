package top_products_by_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}
import org.apache.spark.sql.hive.HiveContext
import utils.InputProcessor

class TopNProductsByCategoryDF(private val hiveContext : HiveContext) {
  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    val windowSpec = Window.
      partitionBy("category").
      orderBy(desc("count"))
    val dataFrame = new InputProcessor(hiveContext).
      createEventsDF(inputFile).
      groupBy("category", "product_name").
      count().
      withColumn("row_number", row_number() over windowSpec).
      cache()
    dataFrame.filter(dataFrame("row_number") < n)

  }

}
