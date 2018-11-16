import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatest._
import Matchers._

class TopNSpendingCountriesTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext

  test("testRDDCalculation") {
    val expected = Array (("England", BigDecimal("1249.54442975586234521045980727649293839931488037109375")),
      ("Russia", BigDecimal("1169.4425687337158166201334097422659397125244140625")),
      ("Austria", BigDecimal("9")))

    val actual = new TopNSpendingCountries(hiveContext).calculateUsingRDD(
      "src/test/resources/topProductsByCategories.csv",
      "src/test/resources/countries_ip.csv", 3)

    assert(actual === expected)
  }

  test("testDFCalculation") {
    val expectedSchema = new StructType().
      add(StructField("country_name", StringType, nullable = true)).
      add(StructField("total_purchases", DecimalType(38,12), nullable = true))
    val expectedData = Seq(
      Row("England", BigDecimal("1249.544429755863")),
      Row("Russia", BigDecimal("1169.442568733716")),
      Row("Austria", BigDecimal("9.000000000000")))
    val expectedDF = hiveContext.createDataFrame(sc.parallelize(expectedData), expectedSchema)

    val actualDF = new TopNSpendingCountries(hiveContext).calculateUsingDF(
      "src/test/resources/topProductsByCategories.csv",
      "src/test/resources/countries_ip.csv", 3)
    assert(actualDF.schema === expectedSchema)
    actualDF.collect() should contain theSameElementsAs  expectedDF.collect()
  }


}
