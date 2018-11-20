package utils

object Runner {
  def run(tableName:String , calculator: Calculator, args:Array[String], n: Int) :Unit = {
    val dataFrame = calculator.calculate(args, n)
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/hadoop"
    new DBOutputWriter(prop, url, tableName).writeDataFrame(dataFrame)
  }

}
