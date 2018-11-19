package utils

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class InputProcessor(private val hiveContext: HiveContext) {

  def readFromFile(inputFile: String): RDD[Array[String]] = {
    val separator = ","

    hiveContext.sparkContext.textFile(inputFile)
      .map(line => line.split(separator))
  }

  def createEventsDF(inputFile: String) : DataFrame = {
    val schema = new StructType().
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("product_price", DecimalType(38, 12), nullable = true)).
      add(StructField("purchase_date", TimestampType, nullable = true)).
      add(StructField("category", StringType, nullable = true)).
      add(StructField("ip_address", StringType, nullable = true))

    val stringsToRow: Array[String] => Row = t => Row(t(0), BigDecimal(t(1)), Timestamp.valueOf(t(2)), t(3), t(4))

    hiveContext.createDataFrame(readFromFile(inputFile).map(stringsToRow), schema)
  }

  def createCountriesDF(inputFile: String) : DataFrame = {
    val schema = new StructType().
      add(StructField("network", StringType, nullable = true)).
      add(StructField("country_iso_code", StringType, nullable = true)).
      add(StructField("country_name", StringType, nullable = true))

    val stringsToRow: Array[String] => Row = t => Row(t(0), t(1), t(2))

    hiveContext.createDataFrame(readFromFile(inputFile).map(stringsToRow), schema)

  }

}
