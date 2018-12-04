package utils

import java.io.Serializable

class CountryByIpFinder(private val lowHighIpCountries: Array[LowHighIpCountry]) extends Serializable {

  def findCountryByIp(ip: String): Option[String] = {
    binarySearch(0, lowHighIpCountries.length, ip)
  }

  private def binarySearch(fromIdx: Int, toIdx: Int, ip: String): Option[String] = {
    if (fromIdx == toIdx) return scala.None
    val ipLongRepresentation = CountriesIpDataProcessor.ipToLong(ip)
    val medium = (toIdx + fromIdx) / 2
    if (ipLongRepresentation <= lowHighIpCountries(medium).highIp &&
      ipLongRepresentation >= lowHighIpCountries(medium).lowIp) {
        Option(lowHighIpCountries(medium).country)
    } else if (ipLongRepresentation > lowHighIpCountries(medium).highIp) {
        binarySearch(fromIdx, medium, ip)
    } else if (ipLongRepresentation < lowHighIpCountries(medium).lowIp) {
        binarySearch(medium + 1, toIdx, ip)
    } else scala.None
  }
}

case class LowHighIpCountry(lowIp: Long, highIp: Long, country: String)
