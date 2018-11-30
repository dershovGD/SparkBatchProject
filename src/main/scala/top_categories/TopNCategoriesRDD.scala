package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.{Calculator, InputProcessor, Runner, SchemaManager}

class TopNCategoriesRDD(private val inputFiles: Array[String]) extends Calculator{
  val eventsFile = inputFiles(0)
  def calculateUsingRDD(hiveContext: HiveContext, n: Int): Array[CategoryCount] = {
    new InputProcessor(hiveContext.sparkContext).readEvents(eventsFile).
      map(event => (event.category, 1L)).
      reduceByKey(_ + _).
      map(r => CategoryCount(r._1, r._2)).
      sortBy(_.count, ascending = false).
      take(n)

  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    val array = calculateUsingRDD(hiveContext, n)
    new SchemaManager(hiveContext).createTopCategoriesDF(array)
  }
}

case class CategoryCount(category:String, count: Long)

object TopNCategoriesRDD {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNCategoriesRDD(args)
    Runner.run("spark_top_categories_rdd", calculator, 10)
  }
}