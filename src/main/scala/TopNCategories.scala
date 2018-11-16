import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

class TopNCategories(private val hiveContext : HiveContext) {
  def calculateUsingRDD(inputFile: String, n: Int): Array[(String, Int)] = {
    new InputOutputProcessor(hiveContext).readFromFile(inputFile).
      map(line => (line(3), 1)).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(n)

  }

  def calculateUsingDF(inputFile: String, n: Int): DataFrame = {
    new InputOutputProcessor(hiveContext).createEventsDF(inputFile).
      groupBy("category").
      count().
      sort(desc("count")).
      limit(n)
  }

  def printToFile(fileName: String)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(fileName)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}