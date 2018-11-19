package top_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNCategoriesRDDTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val expected = Array (("ZXF", 8), ("KMC", 4), ("bWD", 4))

    val actual = new TopNCategoriesRDD(hiveContext).calculateUsingRDD("src/test/resources/topCategories.csv", 3)

    assert(actual === expected)
  }

}
