package utils
import java.io.Serializable

import org.apache.commons.net.util.SubnetUtils

class CountryByIpFinder(private val countriesIp : Array[NetworkCountry]) extends Serializable {
  private val comparator = (n1:LowHighIpCountry, n2: LowHighIpCountry) => {
    if (n1.highIp < n2.lowIp) false
    else true
  }

  private val mapper = (nc: NetworkCountry) => {
    val info = new SubnetUtils(nc.network).getInfo
    LowHighIpCountry(
      ipToLong(info.getLowAddress),
      ipToLong(info.getHighAddress),
      nc.country)
  }

  private def ipToLong (ip : String) : Long = {
    val parts = ip.split("\\.")
    Integer.parseInt(parts(3)) +
      256L*Integer.parseInt(parts(2)) +
      256L*256L*Integer.parseInt(parts(1)) +
      256L*256L*256L*Integer.parseInt(parts(0))
  }

  def findCountryByIp(ip: String) : Option[String] = {
    val lowHighIpCountries = countriesIp.map(mapper).sortWith(comparator)
    binarySearch(lowHighIpCountries, ip)
  }

  private def binarySearch(array: Array[LowHighIpCountry], ip: String) : Option[String] = {
    if (array.length == 0) return scala.None
    val ipLongRepresentation = ipToLong(ip)
    val medium = array.length / 2
    if (ipLongRepresentation <= array(medium).highIp && ipLongRepresentation >= array(medium).lowIp){
      Option(array(medium).country)
    } else if (ipLongRepresentation > array(medium).highIp){
      binarySearch(array.slice(0 , medium), ip)
    } else if (ipLongRepresentation < array(medium).lowIp){
      binarySearch(array.slice(medium+1, array.length), ip)
    } else scala.None
  }
}

case class NetworkCountry(network: String, country:String)
case class LowHighIpCountry(lowIp: Long, highIp: Long, country: String)
