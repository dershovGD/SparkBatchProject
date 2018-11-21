package utils

import org.apache.spark.sql.{DataFrame, SaveMode}

class DBOutputWriter(private val properties: java.util.Properties, private val url: String, private val tableName: String) {
  def writeDataFrame(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Append).jdbc(url, tableName, properties)
  }

}
