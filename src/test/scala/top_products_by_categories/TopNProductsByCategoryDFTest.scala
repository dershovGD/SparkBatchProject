package top_products_by_categories

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class TopNProductsByCategoryDFTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

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
    val inputFiles = Array("src/test/resources/topProductsByCategories.csv")

    val actualDF = new TopNProductsByCategoryDF(inputFiles).calculateUsingDF(hiveContext, 3)
    assert(actualDF.schema === expectedSchema)
    actualDF.collect() should contain theSameElementsAs  expectedDF.collect()
  }

}
