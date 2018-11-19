package top_categories

import org.apache.spark.sql.hive.HiveContext
import utils.InputProcessor

class TopNCategoriesRDD(private val hiveContext : HiveContext) {
  def calculateUsingRDD(inputFile: String, n: Int): Array[(String, Int)] = {
    new InputProcessor(hiveContext).readFromFile(inputFile).
      map(line => (line(3), 1)).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(n)

  }

}