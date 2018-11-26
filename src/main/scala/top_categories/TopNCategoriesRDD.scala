package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.{Calculator, InputProcessor, Runner, SchemaManager}

class TopNCategoriesRDD(private val inputFiles: Array[String]) extends Calculator{
  val eventsFile = inputFiles(0)
  def calculateUsingRDD(hiveContext: HiveContext, n: Int): Array[(String, Long)] = {
    new InputProcessor(hiveContext.sparkContext).readFromFile(eventsFile).
      map(line => (line(3), 1L)).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(n)

  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    val array = calculateUsingRDD(hiveContext, n)
    new SchemaManager(hiveContext).createTopCategoriesDF(array)
  }
}

object TopNCategoriesRDD {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNCategoriesRDD(args)
    Runner.run("spark_top_categories_rdd", calculator, 10)
  }
}