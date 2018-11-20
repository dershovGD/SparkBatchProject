package top_products_by_categories

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import utils.{InputProcessor, NElementsSet}

class TopNProductsByCategoryRDD(private val hiveContext : HiveContext) {
  def calculateUsingRDD(inputFile: String, n: Int): RDD[(String, String, Long)] = {
    //Create a SparkContext to initialize Spark

    val data = new InputProcessor(hiveContext.sparkContext).readFromFile(inputFile)
    data.map(line => (Tuple2(line(3), line(0)), 1L)).
      reduceByKey(_ + _).
      map(record => (record._1._1, Tuple2(record._1._2, record._2))).
      aggregateByKey(new NElementsSet[(String, Long)](n, Ordering.by(_._2)))((set, tuple) => set += tuple,
        (set1, set2) => set1 ++= set2).
      flatMap(tuple => tuple._2.listWithKey(tuple._1))
      .map(e => (e._1, e._2._1, e._2._2))
  }

}
