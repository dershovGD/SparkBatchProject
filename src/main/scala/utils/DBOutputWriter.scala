package utils

import org.apache.spark.sql.DataFrame

class DBOutputWriter(private val properties: java.util.Properties, private val url: String, private val tableName: String) {
  def writeDataFrame(df: DataFrame): Unit = {
    df.write.mode("append").jdbc(url, tableName, properties)
  }

}
