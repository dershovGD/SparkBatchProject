package utils

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class CountriesIpDataProcessorTest extends FunSuite {

  test("testProcessCountries") {
    val inputArray = Array(
      Country(Array("234.42.135.0/24", "ru", "Russia")),
      Country(Array("9.88.152.0/24", "en", "England")),
      Country(Array("62.121.0.0/16", "de", "Germany")),
      Country(Array("150.0.0.0/8", "at", "Austria")),
      Country(Array("0.88.152.0/24", "us", "USA")))

    val expectedArray = Array(
      LowHighIpCountry(3928655617L,3928655870L,"Russia"),
        LowHighIpCountry(2516582401L,2533359614L,"Austria"),
        LowHighIpCountry(1048117249L,1048182782L,"Germany"),
        LowHighIpCountry(156801025L,156801278L,"England"),
        LowHighIpCountry(5806081L,5806334L,"USA")
    )

    val countriesIpSorted = CountriesIpDataProcessor.processCountries(inputArray)
    countriesIpSorted should contain theSameElementsAs expectedArray
  }

}
