import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatest._
import Matchers._

class TopNCategoriesTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll{
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext


  test("testRDDCalculation") {
    val expected = Array (("ZXF", 8), ("KMC", 4), ("bWD", 4))

    val actual = new TopNCategories(hiveContext).calculateUsingRDD("src/test/resources/topCategories.csv", 3)

    assert(actual === expected)
  }

  test("testDFCalculation") {
    val expectedSchema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))
    val expectedData = Seq(
      Row("ZXF", 8L),
      Row("bWD", 4L),
      Row("KMC", 4L))

    val expectedDF = hiveContext.createDataFrame(sc.parallelize(expectedData), expectedSchema)

    val actual = new TopNCategories(hiveContext).calculateUsingDF("src/test/resources/topCategories.csv", 3)
    assert(actual.schema === expectedDF.schema)
    actual.collect() should contain theSameElementsAs  expectedDF.collect()
  }

}
