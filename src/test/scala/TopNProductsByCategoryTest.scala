import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatest._
import Matchers._

class TopNProductsByCategoryTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val devices = List(("mouse",6), ("monitor",10), ("iphone",11))
    val toys = List(("teddyBear",5))
    val autos = List(("citroen",3), ("peugeot",5), ("reno",4))

    val expected = Array(("device", devices), ("toy", toys), ("auto", autos))

    val actualRDD = new TopNProductsByCategory(hiveContext).calculateUsingRDD("src/test/resources/topProductsByCategories.csv", 3)
    val actualArray = actualRDD.mapValues(_.toList).collect()
    assert (actualArray === expected)

  }

  test("testDFCalculation") {
    val expectedSchema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false)).
      add(StructField("row_number", IntegerType, nullable = true))
    val expectedData = Seq(
      Row("auto", "peugeot", 5L, 1),
      Row("auto", "reno", 4L, 2),
      Row("device", "iphone", 11L, 1),
      Row("device", "monitor", 10L, 2),
      Row("toy", "teddyBear", 5L, 1))
    val expectedDF = hiveContext.createDataFrame(sc.parallelize(expectedData), expectedSchema)

    val actualDF = new TopNProductsByCategory(hiveContext).calculateUsingDF("src/test/resources/topProductsByCategories.csv", 3)
    assert(actualDF.schema === expectedSchema)
    actualDF.collect() should contain theSameElementsAs  expectedDF.collect()
  }


}
