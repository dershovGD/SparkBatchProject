import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.{SparkConf, SparkContext}

object HiveContextMaintainer {
  private val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("test")


  def getHiveContext: HiveContext = {
    val sc = SparkContext.getOrCreate(conf)
    TestHive
  }

}
