package utils

import org.scalatest.FunSuite

class CountryByIpFinderTest extends FunSuite {
  test("testEvaluation") {
    val inputArray = Array(
      LowHighIpCountry(3928655617L,3928655870L,"Russia"),
      LowHighIpCountry(2516582401L,2533359614L,"Austria"),
      LowHighIpCountry(1048117249L,1048182782L,"Germany"),
      LowHighIpCountry(156801025L,156801278L,"England"),
      LowHighIpCountry(5806081L,5806334L,"USA")
    )

    val finder = new CountryByIpFinder(inputArray)

    assert(finder.findCountryByIp( "62.121.0.56").get === "Germany")
    assert(finder.findCountryByIp("61.121.0.56") === None)
    assert(finder.findCountryByIp("234.42.135.1").get === "Russia")
  }

}
