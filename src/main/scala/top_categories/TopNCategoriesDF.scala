package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.hive.HiveContext
import utils.InputProcessor

class TopNCategoriesDF(private val hiveContext : HiveContext) {
  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    new InputProcessor(hiveContext).createEventsDF(inputFile).
      groupBy("category").
      count().
      sort(desc("count")).
      limit(n)
  }

}
