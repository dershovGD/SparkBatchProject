import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

class TopNProductsByCategory(private val hiveContext : HiveContext) {
  def calculateUsingRDD(inputFile: String, n: Int): RDD[(String, NElementsSet[(String, Int)])] = {
    //Create a SparkContext to initialize Spark

    val data = new InputOutputProcessor(hiveContext).readFromFile(inputFile)
    data.map(line => (Tuple2(line(3), line(0)), 1)).
      reduceByKey(_ + _).
      map(record => (record._1._1, Tuple2(record._1._2, record._2))).
      aggregateByKey(new NElementsSet[(String, Int)](n, Ordering.by(_._2)))((set, tuple) => set += tuple,
        (set1, set2) => set1 ++= set2)

  }

  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    val windowSpec = Window.
      partitionBy("category").
      orderBy(desc("count"))
    val dataFrame = new InputOutputProcessor(hiveContext).
      createEventsDF(inputFile).
      groupBy("category", "product_name").
      count().
      withColumn("row_number", row_number() over windowSpec).
      cache()
    dataFrame.filter(dataFrame("row_number") < n)

  }

}
