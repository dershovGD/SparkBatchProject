package utils

import org.scalatest.FunSuite

class CountryByIpFinderTest extends FunSuite {
  test("testEvaluation") {
    val inputArray = Array(
      NetworkCountry("234.42.135.0/24", "Russia"),
      NetworkCountry("9.88.152.0/24", "England"),
      NetworkCountry("62.121.0.0/16", "Germany"),
      NetworkCountry("150.0.0.0/8", "Austria"),
      NetworkCountry("0.88.152.0/24", "USA"))

    assert(new CountryByIpFinder(inputArray).findCountryByIp("62.121.0.56").get === "Germany")
    assert(new CountryByIpFinder(inputArray).findCountryByIp("61.121.0.56") === None)
    assert(new CountryByIpFinder(inputArray).findCountryByIp("234.42.135.1").get === "Russia")
  }

}
