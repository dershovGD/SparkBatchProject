package top_spending_countries

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNSpendingCountriesRDDTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val expected = Array (("England", BigDecimal("1243")),
      ("Russia", BigDecimal("1165.8")),
      ("Austria", BigDecimal("9")))

    val actual = new TopNSpendingCountriesRDD(hiveContext).calculateUsingRDD(
      "src/test/resources/topProductsByCategories.csv",
      "src/test/resources/countries_ip.csv", 3)

    assert(actual === expected)
  }
}
