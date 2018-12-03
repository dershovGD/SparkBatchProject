package utils

import org.scalatest.FunSuite

class CountryByIpFinderTest extends FunSuite {
  test("testEvaluation") {
    val inputArray = Array(
      Country(Array("234.42.135.0/24", "ru", "Russia")),
      Country(Array("9.88.152.0/24", "en", "England")),
      Country(Array("62.121.0.0/16", "de", "Germany")),
      Country(Array("150.0.0.0/8", "at", "Austria")),
      Country(Array("0.88.152.0/24", "us", "USA")))

    val finder = new CountryByIpFinder(inputArray)

    assert(finder.findCountryByIp( "62.121.0.56").get === "Germany")
    assert(finder.findCountryByIp("61.121.0.56") === None)
    assert(finder.findCountryByIp("234.42.135.1").get === "Russia")
  }

}
