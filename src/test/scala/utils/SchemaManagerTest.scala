package utils

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class SchemaManagerTest extends FunSuite {

  test("testCreateTopProductsByCategoriesDF") {
    val schema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))


    val expectedData = Seq(
      ("auto", "peugeot", 5L),
      ("device", "iphone", 11L),
      ("toy", "teddyBear", 5L))
    val rdd = TestHive.sparkContext.parallelize(expectedData)

    val actualDF = new SchemaManager(TestHive, new InputProcessor(TestHive.sparkContext)).
      createTopProductsByCategoriesDF(rdd)
    assert(actualDF.schema === schema)
    actualDF.collect() should contain theSameElementsAs expectedData
  }

}
