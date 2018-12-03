package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import top_categories.CategoryCount
import top_products_by_categories.CategoryProductCount

import scala.collection.JavaConversions._

class SchemaManager(private val hiveContext: HiveContext) {
  def createEventsDF(inputFile: String): DataFrame = {
    val schema = new StructType().
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("product_price", DecimalType(38, 12), nullable = true)).
      add(StructField("purchase_date", TimestampType, nullable = true)).
      add(StructField("category", StringType, nullable = true)).
      add(StructField("ip_address", StringType, nullable = true))

    val rows = new InputProcessor(hiveContext.sparkContext).
      readEvents(inputFile).
      map(event => Row(event.productName, event.productPrice, event.purchaseDate, event.category, event.ipAddress))

    hiveContext.createDataFrame(rows, schema)
  }

  def createTopCategoriesDF(topCategories: Array[CategoryCount]): DataFrame = {
    val schema: StructType = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))
    val rows = topCategories.
      map(record => Row(record.category, record.count))
    hiveContext.createDataFrame(rows.toSeq, schema)
  }

  def createTopProductsByCategoriesDF(topProductsByCategories: RDD[CategoryProductCount]): DataFrame = {
    val schema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("product_name", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))

    val rows = topProductsByCategories.
      map(t => Row(t.category, t.productName, t.count))
    hiveContext.createDataFrame(rows, schema)
  }

  def createTopSpendingCountriesDF(topSpendingCountries: Array[(String, BigDecimal)]): DataFrame = {
    val schema = new StructType().
      add(StructField("country_name", StringType, nullable = true)).
      add(StructField("total_purchases", DecimalType(38, 12), nullable = true))

    val rows = topSpendingCountries.map(e => Row(e._1, e._2))
    hiveContext.createDataFrame(rows.toSeq, schema)
  }
}
