package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.{Calculator, InputProcessor, Runner, SchemaManager}

class TopNCategoriesRDD(private val hiveContext: HiveContext) extends Calculator{
  def calculateUsingRDD(inputFile: String, n: Int): Array[(String, Long)] = {
    new InputProcessor(hiveContext.sparkContext).readFromFile(inputFile).
      map(line => (line(3), 1L)).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(n)

  }

  override def calculate(args: Array[String], n: Int): DataFrame = {
    val array = calculateUsingRDD(args(0), n)
    new SchemaManager(hiveContext).createTopCategoriesDF(array)
  }
}

object TopNCategoriesRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("TopNCategoriesRDD")
      .set("spark.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val sc = new SparkContext(conf)
    val calculator = new TopNCategoriesRDD(new HiveContext(sc))

    Runner.run("spark_top_categories_rdd", calculator, args, 10)
  }
}