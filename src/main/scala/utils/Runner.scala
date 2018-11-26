package utils

import java.io.{BufferedReader, FileReader}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object Runner {
  def run(appName:String, calculator: Calculator, n: Int) :Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName(appName)
      .set("spark.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val hiveContext = new HiveContext(sc)

    val dataFrame = calculator.calculate(hiveContext, n)

    val prop = new java.util.Properties
    val reader = new BufferedReader(new FileReader(SparkFiles.get("properties.conf")))
    prop.load(reader)
    val url = prop.getProperty("url")
    new DBOutputWriter(prop, url, appName).writeDataFrame(dataFrame)
  }

}
