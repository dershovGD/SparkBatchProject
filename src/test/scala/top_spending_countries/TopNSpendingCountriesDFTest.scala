package top_spending_countries

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class TopNSpendingCountriesDFTest extends FunSuite {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testDFCalculation") {
    val expectedSchema = new StructType().
      add(StructField("country_name", StringType, nullable = true)).
      add(StructField("total_purchases", DecimalType(38,12), nullable = true))
    val expectedData = Seq(
      Row("England", BigDecimal("1243")),
      Row("Russia", BigDecimal("1165.8")),
      Row("Austria", BigDecimal("9.000000000000")))
    val expectedDF = hiveContext.createDataFrame(sc.parallelize(expectedData), expectedSchema)

    val actualDF = new TopNSpendingCountriesDF(hiveContext).calculateUsingDF(
      "src/test/resources/topProductsByCategories.csv",
      "src/test/resources/countries_ip.csv", 3)
    assert(actualDF.schema === expectedSchema)
    actualDF.collect() should contain theSameElementsAs  expectedDF.collect()
  }

}
