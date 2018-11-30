package top_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNCategoriesRDDTest extends FunSuite {
  private val hiveContext = TestHive

  test("testRDDCalculation") {
    val expected = Array (
      CategoryCount("ZXF", 8L),
      CategoryCount("KMC", 4L),
      CategoryCount("bWD", 4L))
    val inputFiles = Array("src/test/resources/topCategories")

    val actual = new TopNCategoriesRDD(inputFiles).calculateUsingRDD(hiveContext, 3)

    assert(actual === expected)
  }

}
