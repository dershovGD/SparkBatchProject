package top_products_by_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNProductsByCategoryRDDTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val devices = List(("mouse",6L), ("monitor",10L), ("iphone",11L))
    val toys = List(("teddyBear",5L))
    val autos = List(("citroen",3L), ("peugeot",5L), ("reno",4L))

    val expected = Array(("device", "mouse" ,6L), ("device", "monitor", 10L), ("device", "iphone", 11L),
      ("toy", "teddyBear", 5L),
      ("auto", "citroen", 3L), ("auto", "peugeot", 5L), ("auto", "reno", 4L))

    val actualRDD = new TopNProductsByCategoryRDD(hiveContext).calculateUsingRDD("src/test/resources/topProductsByCategories.csv", 3)
    val actualArray = actualRDD.collect()
    assert (actualArray === expected)

  }

}
