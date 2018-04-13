//package com.corp.spark.tsfile
//
//import Constant
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
//import org.apache.spark.sql.SQLContext
//import org.junit.{After, Assert, Before, Test}
//import org.scalatest.junit.JUnitSuite
//
///**
//  * Created by qjl on 16-9-4.
//  * IT need starting tomcat and deploy tsfile-rest
//  */
//class TSFileSuit extends JUnitSuite {
//  private val testFile = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
//  private val csvPath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.csv"
//  private val tsfilePath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
//  private val errorPath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/errortest.tsfile"
//  private var sqlContext: SQLContext = _
//
//  @Before
//  def before(): Unit = {
////    new CreateTSFile().createTSFile(csvPath, tsfilePath, errorPath)
//    sqlContext = new SQLContext(new SparkContext("local[2]", "TSFileSuit"))
//
//  }
//
//  @After
//  def after(): Unit = {
//    if(sqlContext != null) {
//      sqlContext.sparkContext.stop()
//    }
//  }
//
//  @Test
//  def longSchema(): Unit = {
//    val df = sqlContext.read.tsfile(Map[String, String]("path" -> testFile))
//
//    val expected = StructType(Seq(
//      StructField(Constant.TIMESTAMP, LongType, nullable = true),
//      StructField("d1__s3", LongType, nullable = true),
//      StructField("d1__s1", LongType, nullable = true),
//      StructField("d1__s2", LongType, nullable = true),
//      StructField("d2__s3", LongType, nullable = true),
//      StructField("d2__s1", LongType, nullable = true),
//      StructField("d2__s2", LongType, nullable = true)
//    ))
//    Assert.assertEquals(expected, df.schema)
//  }
//
//  @Test
//  def longData(): Unit = {
//    val df = sqlContext.read.tsfile(Map[String, String]("path" -> testFile))
//
//    Assert.assertEquals(1, df.collect().length)
//  }
//
//  @Test
//  def longSql(): Unit = {
//    val createSql = "create temporary view tsfile using com.corp.spark.tsfile options(path = \""+ testFile + "\")"
//
//    sqlContext.sql(createSql)
//    val df = sqlContext.sql("select * from tsfile")
//
//    Assert.assertEquals(df.collect().length, 1)
//  }
//
//
//  @Test
//  def unionSchema(): Unit = {
//    val df = sqlContext.read.tsfile(Map[String, String]("path" -> testFile, "union" -> "true"))
//
//    val expected = StructType(Seq(
//      StructField(Constant.TIMESTAMP, LongType, nullable = true),
//      StructField(Constant.DEVICEID, StringType, nullable = true),
//      StructField("s3__LONG", LongType, nullable = true),
//      StructField("s1__LONG", LongType, nullable = true),
//      StructField("s2__LONG", LongType, nullable = true)
//    ))
//    Assert.assertEquals(expected, df.schema)
//  }
//
//  @Test
//  def unionData(): Unit = {
//    val df = sqlContext.read.tsfile(Map[String, String]("path" -> testFile, "union" -> "true"))
//
//    Assert.assertEquals(df.collect().length, 1)
//  }
//
//  @Test
//  def unionSql(): Unit = {
//    val createSql = "create temporary view tsfile using com.corp.spark.tsfile options(path = \""+ testFile + "\", union = \"true\")"
//
//    sqlContext.sql(createSql)
//    val df = sqlContext.sql("select * from tsfile")
//    df.foreach(f => println(f))
//
//    Assert.assertEquals(1, df.collect().length)
//  }
//
//
//}
