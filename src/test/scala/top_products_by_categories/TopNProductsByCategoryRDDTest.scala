package top_products_by_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNProductsByCategoryRDDTest extends FunSuite {
  private val hiveContext = TestHive

  test("testRDDCalculation") {
    val expected = Array(
      CategoryProductCount("device", "mouse" ,6L),
      CategoryProductCount("device", "monitor", 10L),
      CategoryProductCount("device", "iphone", 11L),
      CategoryProductCount("toy", "teddyBear", 5L),
      CategoryProductCount("auto", "citroen", 3L),
      CategoryProductCount("auto", "peugeot", 5L),
      CategoryProductCount("auto", "reno", 4L))

    val inputFiles = Array("src/test/resources/topProductsByCategories.csv")


    val actualRDD = new TopNProductsByCategoryRDD(inputFiles).calculateUsingRDD(hiveContext, 3)
    val actualArray = actualRDD.collect()
    assert (actualArray === expected)

  }

}
