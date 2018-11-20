package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class InputProcessor(private val sc : SparkContext) {

  def readFromFile(inputFile: String): RDD[Array[String]] = {
    val separator = ","

    sc.textFile(inputFile)
      .map(line => line.split(separator))
  }

}
