package top_products_by_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNProductsByCategoryRDDTest extends FunSuite {
  private val hiveContext = TestHive

  test("testRDDCalculation") {
    val devices = List(("mouse",6L), ("monitor",10L), ("iphone",11L))
    val toys = List(("teddyBear",5L))
    val autos = List(("citroen",3L), ("peugeot",5L), ("reno",4L))

    val expected = Array(("device", "mouse" ,6L), ("device", "monitor", 10L), ("device", "iphone", 11L),
      ("toy", "teddyBear", 5L),
      ("auto", "citroen", 3L), ("auto", "peugeot", 5L), ("auto", "reno", 4L))

    val inputFiles = Array("src/test/resources/topProductsByCategories.csv")


    val actualRDD = new TopNProductsByCategoryRDD(inputFiles).calculateUsingRDD(hiveContext, 3)
    val actualArray = actualRDD.collect()
    assert (actualArray === expected)

  }

}
