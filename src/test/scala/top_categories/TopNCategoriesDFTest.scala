package top_categories

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class TopNCategoriesDFTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testDFCalculation") {
    val expectedSchema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))
    val expectedData = Seq(
      Row("ZXF", 8L),
      Row("bWD", 4L),
      Row("KMC", 4L))

    val expectedDF = hiveContext.createDataFrame(sc.parallelize(expectedData), expectedSchema)
    val inputFiles = Array("src/test/resources/topCategories")

    val actual = new TopNCategoriesDF(inputFiles).calculateUsingDF(hiveContext, 3)
    assert(actual.schema === expectedDF.schema)
    actual.collect() should contain theSameElementsAs  expectedDF.collect()
  }

}
