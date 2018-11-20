package utils

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import collection.JavaConversions._

class SchemaManager(private val hiveContext: HiveContext) {
  def createEventsDF(inputFile: String): DataFrame = {
    val schema = new StructType().
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("product_price", DecimalType(38, 12), nullable = true)).
      add(StructField("purchase_date", TimestampType, nullable = true)).
      add(StructField("category", StringType, nullable = true)).
      add(StructField("ip_address", StringType, nullable = true))

    val stringsToRow: Array[String] => Row = t => Row(t(0), BigDecimal(t(1)), Timestamp.valueOf(t(2)), t(3), t(4))
    val rows = new InputProcessor(hiveContext.sparkContext).readFromFile(inputFile).map(stringsToRow)

    hiveContext.createDataFrame(rows, schema)
  }

  def createCountriesDF(inputFile: String): DataFrame = {
    val schema = new StructType().
      add(StructField("network", StringType, nullable = true)).
      add(StructField("country_iso_code", StringType, nullable = true)).
      add(StructField("country_name", StringType, nullable = true))

    val stringsToRow: Array[String] => Row = t => Row(t(0), t(1), t(2))
    val rows = new InputProcessor(hiveContext.sparkContext).readFromFile(inputFile).map(stringsToRow)

    hiveContext.createDataFrame(rows, schema)
  }

  def createTopCategoriesDF(topCategories: Array[(String, Long)]): DataFrame = {
    val schema: StructType = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))
    val rows = topCategories.map(e => Row(e._1, e._2))
    hiveContext.createDataFrame(rows.toSeq, schema)
  }

  def createTopProductsByCategoriesDF(topProductsByCategories: RDD[(String, String, Long)]): DataFrame = {
    val schema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))

    val rows = topProductsByCategories.map(t => Row(t._1, t._2, t._3)).collect()
    hiveContext.createDataFrame(rows.toSeq, schema)
  }

  def createTopSpendingCountriesDF(topSpendingCountries: Array[(String, BigDecimal)]): DataFrame = {
    val schema = new StructType().
      add(StructField("country_name", StringType, nullable = true)).
      add(StructField("total_purchases", DecimalType(38, 12), nullable = true))

    val rows = topSpendingCountries.map(e => Row(e._1, e._2))
    hiveContext.createDataFrame(rows.toSeq, schema)
  }
}
