package utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import top_categories.CategoryCount
import top_products_by_categories.CategoryProductCount

class SchemaManagerTest extends FunSuite {

  test("testCreateTopProductsByCategoriesDF") {
    val schema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))


    val expectedData = Array(
      CategoryProductCount("auto", "peugeot", 5L),
      CategoryProductCount("device", "iphone", 11L),
      CategoryProductCount("toy", "teddyBear", 5L))
    val rdd = TestHive.sparkContext.parallelize(expectedData)

    val actualDF = new SchemaManager(TestHive).
      createTopProductsByCategoriesDF(rdd)
    assert(actualDF.schema === schema)
    assert(actualDF.count == 3)
    actualDF.collect() should contain theSameElementsAs expectedData.
      map(t => Row(t.category, t.productName, t.count))
  }

  test("testCreateTopCategoriesDF") {
    val schema: StructType = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))


    val expectedData = Array(
      CategoryCount("auto", 5L),
      CategoryCount("device", 11L),
      CategoryCount("toy", 5L))

    val actualDF = new SchemaManager(TestHive).
      createTopCategoriesDF(expectedData)
    assert(actualDF.schema === schema)
    assert(actualDF.count == 3)
    actualDF.collect() should contain theSameElementsAs expectedData.map(r => Row(r.category, r.count))
  }

}
