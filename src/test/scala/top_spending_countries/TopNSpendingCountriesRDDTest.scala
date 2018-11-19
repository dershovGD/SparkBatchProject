package top_spending_countries

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class TopNSpendingCountriesRDDTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val expected = Array (("England", BigDecimal("1249.54442975586234521045980727649293839931488037109375")),
      ("Russia", BigDecimal("1169.4425687337158166201334097422659397125244140625")),
      ("Austria", BigDecimal("9")))

    val actual = new TopNSpendingCountriesRDD(hiveContext).calculateUsingRDD(
      "src/test/resources/topProductsByCategories.csv",
      "src/test/resources/countries_ip.csv", 3)

    assert(actual === expected)
  }
}
