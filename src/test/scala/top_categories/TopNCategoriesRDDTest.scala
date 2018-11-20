package top_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNCategoriesRDDTest extends FunSuite {
  private val hiveContext = TestHive

  test("testRDDCalculation") {
    val expected = Array (("ZXF", 8L), ("KMC", 4L), ("bWD", 4L))

    val actual = new TopNCategoriesRDD(hiveContext).calculateUsingRDD("src/test/resources/topCategories", 3)

    assert(actual === expected)
  }

}
