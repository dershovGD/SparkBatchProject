package utils

import org.apache.commons.net.util.SubnetUtils

object CountriesIpDataProcessor {
  def processCountries(networkCountries : Array[Country]): Array[LowHighIpCountry] = {
    val comparator = (n1: LowHighIpCountry, n2: LowHighIpCountry) => {
      if (n1.highIp < n2.lowIp) false
      else true
    }

    val mapper = (nc: Country) => {
      val info = new SubnetUtils(nc.network).getInfo
      LowHighIpCountry(
        ipToLong(info.getLowAddress),
        ipToLong(info.getHighAddress),
        nc.countryName)
    }

    networkCountries.map(mapper).sortWith(comparator)
  }
  def ipToLong(ip: String): Long = {
    val parts = ip.split("\\.")
    Integer.parseInt(parts(3)) +
      256L * Integer.parseInt(parts(2)) +
      256L * 256L * Integer.parseInt(parts(1)) +
      256L * 256L * 256L * Integer.parseInt(parts(0))
  }

}
