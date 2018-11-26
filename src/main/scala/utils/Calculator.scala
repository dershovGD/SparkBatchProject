package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

trait Calculator {
  def calculate(hiveContext: HiveContext, n:Int) : DataFrame

}
