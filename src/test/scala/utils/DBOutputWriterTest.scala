package utils

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DBOutputWriterTest extends FunSuite with BeforeAndAfterAll{
  private val hiveContext = TestHive
  private val sc = hiveContext.sparkContext
  private var conn : Connection = _

  override def beforeAll(): Unit = {
    conn = DriverManager.getConnection("jdbc:h2:mem:play")
    conn.prepareStatement("create table sample_data_table (category varchar(5), count bigint)")
  }

  test("testWriteDataFrame") {
    writeDFtoDB()
    readAndAssert()
  }

  private def writeDFtoDB() : Unit = {
    val expectedSchema = new StructType().
      add(StructField("category", StringType, nullable = true)).
      add(StructField("count", LongType, nullable = false))
    val expectedData = Seq(
      Row("ZXF", 8L))

    val dataFrame = hiveContext.createDataFrame(sc.parallelize(expectedData), expectedSchema)

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.h2.Driver")

    val url = "jdbc:h2:mem:play;MODE=MYSQL;DATABASE_TO_UPPER=FALSE"

    val table = "sample_data_table"

    val outputWriter = new DBOutputWriter(prop, url, table)
    outputWriter.writeDataFrame(dataFrame)
  }

  private def readAndAssert() : Unit = {
    val resultSet = conn.prepareStatement("select category, count from sample_data_table").executeQuery()
    assert(resultSet.next())
    assert(resultSet.getString("category") ==="ZXF")
    assert(resultSet.getLong("count") === 8L)
    assert(!resultSet.next())
  }

  override def afterAll(): Unit = {
    conn.prepareStatement("drop table sample_data_table")
    conn.close()
  }

}
