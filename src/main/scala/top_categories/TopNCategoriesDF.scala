package top_categories

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils._

class TopNCategoriesDF(private val hiveContext: HiveContext) extends Calculator{
  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    val schemaManager = new SchemaManager(hiveContext)
    schemaManager.createEventsDF(inputFile).
      groupBy("category").
      count().
      sort(desc("count")).
      limit(n)
  }

  override def calculate(args: Array[String], n: Int): DataFrame = {
    calculateUsingDF(args(0), n)
  }
}

object TopNCategoriesDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("TopNCategoriesDF")
      .set("spark.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val calculator = new TopNCategoriesDF(new HiveContext(sc))

    Runner.run("spark_top_categories_df", calculator, args, 10)
  }

}