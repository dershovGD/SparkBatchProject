package utils

import org.apache.spark.sql.DataFrame

trait Calculator {
  def calculate(args: Array[String], n:Int) : DataFrame

}
