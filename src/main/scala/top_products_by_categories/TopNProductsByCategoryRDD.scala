package top_products_by_categories

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import utils._

class TopNProductsByCategoryRDD(private val inputFiles: Array[String]) extends Calculator {
  val eventsFile = inputFiles(0)

  def calculateUsingRDD(hiveContext: HiveContext, n: Int): RDD[CategoryProductCount] = {
    val data = new InputProcessor(hiveContext.sparkContext).readEvents(eventsFile)
    data.map(event => CategoryProductCount(event.category, event.productName, 1L)).
      keyBy(record => (record.category, record.productName)).
      mapValues(_.count).
      reduceByKey(_ + _).
      map(record => CategoryProductCount(record._1._1, record._1._2, record._2)).
      keyBy(_.category).
      mapValues(record => ProductCount(record.productName, record.count)).
      aggregateByKey(new NElementsSet[ProductCount](n, Ordering.by(_.count)))((set, tuple) => set += tuple,
        (set1, set2) => set1 ++= set2).
      flatMap(tuple => tuple._2.listWithKey(tuple._1))
      .map(record => CategoryProductCount(record._1, record._2.productName, record._2.count))
  }

  override def calculate(hiveContext: HiveContext, n: Int): DataFrame = {
    val rdd = calculateUsingRDD(hiveContext, n)
    new SchemaManager(hiveContext).createTopProductsByCategoriesDF(rdd)
  }
}

case class CategoryProductCount(category: String, productName: String, count: Long)

case class ProductCount(productName: String, count: Long)

object TopNProductsByCategoryRDD {
  def main(args: Array[String]): Unit = {
    val calculator = new TopNProductsByCategoryRDD(args)
    Runner.run("spark_top_products_by_categories_rdd", calculator, 10)
  }

}
