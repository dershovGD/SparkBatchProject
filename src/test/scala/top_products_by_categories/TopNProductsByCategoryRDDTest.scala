package top_products_by_categories

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNProductsByCategoryRDDTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val devices = List(("mouse",6), ("monitor",10), ("iphone",11))
    val toys = List(("teddyBear",5))
    val autos = List(("citroen",3), ("peugeot",5), ("reno",4))

    val expected = Array(("device", devices), ("toy", toys), ("auto", autos))

    val actualRDD = new TopNProductsByCategoryRDD(hiveContext).calculateUsingRDD("src/test/resources/topProductsByCategories.csv", 3)
    val actualArray = actualRDD.mapValues(_.toList).collect()
    assert (actualArray === expected)

  }

}
