//package com.corp.spark.tsfile
//
//import java.util
//
//import com.corp.delta.spark.common._
//import Constant
//import com.google.gson.Gson
//import org.apache.spark.sql.sources._
//import org.apache.spark.sql.types._
//import org.junit.{Assert, Test}
//import org.scalatest.junit.JUnitSuite
//
///**
//  * Created by qjl on 16-9-10.
//  */
//class ConvertUnionTest extends JUnitSuite {
//
//
//  @Test
//  def resolveRequiredColumns(): Unit = {
//    val lists = new util.ArrayList[FieldType]()
//    lists.add(FieldType("d1__s3", "LONG"))
//    lists.add(FieldType("d1__s1", "LONG"))
//    lists.add(FieldType("d1__s2", "INT"))
//    lists.add(FieldType("d2__s3", "LONG"))
//    lists.add(FieldType("d2__s1", "LONG"))
//    lists.add(FieldType("d2__s2", "LONG"))
//    val tableDesResponde = TableDesResponde("", 1, lists)
//    val selectedCols = Array(Constant.TIMESTAMP, "s1__LONG", "s2__INT", "s3__LONG")
//    val filters : Array[Filter] = Array(
//      And(EqualTo("d1__s1", 100), GreaterThan("d2__s2", 100)),
//      GreaterThanOrEqual(Constant.TIMESTAMP, 100)
//    )
//    val actual = ConvertUnion.resolveRequiredColumns(tableDesResponde, selectedCols, filters)
//
//    val expected = StringBuilder.newBuilder
//    expected.append("d1,s3|d1,s1|d1,s2|d2,s3|d2,s1")
//
//    Assert.assertEquals(expected, actual)
//  }
//
//  @Test
//  def toTableDataRequest() {
//    val table = "1472879012412"
//    val path = "tsfile"
//    val selectedCols = Array(Constant.TIMESTAMP, "s1__LONG", "s2__INT", "s3__LONG")
//    val filters : Array[Filter] = Array(
//      And(EqualTo("d1__s1", 100), GreaterThan("d2__s2", 100)),
//      GreaterThanOrEqual(Constant.TIMESTAMP, 100)
//    )
//
//    val lists = new util.ArrayList[FieldType]()
//    lists.add(FieldType("d1__s3", "LONG"))
//    lists.add(FieldType("d1__s1", "LONG"))
//    lists.add(FieldType("d1__s2", "INT"))
//    lists.add(FieldType("d2__s3", "LONG"))
//    lists.add(FieldType("d2__s1", "LONG"))
//    lists.add(FieldType("d2__s2", "LONG"))
//    val tableDesResponde = TableDesResponde("", 1, lists)
//
//    val actual = ConvertUnion.toTableDataRequest(tableDesResponde, table, path, selectedCols, filters)
//
//    val expectedStr =
//      """{"table":"1472879012412","path":"tsfile",
//        |"selectedCols":"d1,s3|d1,s1|d1,s2|d2,s3|d2,s1",
//        |"timeFilter":"null","freqFilter":"null",
//        |"valueFilter":"null"}""".stripMargin
//
//    val gson = new Gson
//    val expected = gson.fromJson(expectedStr, classOf[TableDataRequest])
//
//    Assert.assertEquals(expected, actual)
//  }
//
//  @Test
//  def resolveFilter() {
//    Assert.assertEquals(1, 1)
//  }
//
//  @Test
//  def str() {
//    val a = StringBuilder.newBuilder
//    val b = StringBuilder.newBuilder
//    b.append("aaa")
//    Assert.assertEquals("null", ConvertUnion.str(a))
//    Assert.assertEquals("aaa", ConvertUnion.str(b))
//  }
//
//  @Test
//  def toSqlStructType() {
//
//    val fields : util.ArrayList[FieldType]= new util.ArrayList[FieldType]()
//    fields.add(FieldType("d1__s1", "INT"))
//    fields.add(FieldType("d1__s2", "LONG"))
//    fields.add(FieldType("d1__s3", "FLOAT"))
//    fields.add(FieldType("d2__s1", "INT"))
//    fields.add(FieldType("d2__s2", "LONG"))
//    fields.add(FieldType("d2__s3", "STRING"))
//    val tableDesResponde = TableDesResponde("ok", 1, fields)
//    val actualType = ConvertUnion.toSqlStructType(tableDesResponde)
//
//
//    val expectedFields = Array(
//      StructField(Constant.TIMESTAMP, LongType, nullable = false),
//      StructField(Constant.DEVICEID, StringType, nullable = false),
//      StructField("s1" + Constant.SEPARATOR + "INT", IntegerType, nullable = true),
//      StructField("s2" + Constant.SEPARATOR + "LONG", LongType, nullable = true),
//      StructField("s3" + Constant.SEPARATOR + "FLOAT", FloatType, nullable = true),
//      StructField("s3" + Constant.SEPARATOR + "STRING", StringType, nullable = true)
//    )
//    val expectedType = StructType(expectedFields)
//
//    Assert.assertEquals(actualType, expectedType)
//  }
//
//  @Test
//  def toSqlData() {
//    val fieldData1 = FieldData("d1__s1", "INT", "123")
//    val fieldData2 = FieldData("d1__s2", "LONG", "123")
//    val fieldData3 = FieldData("d1__s2", "FLOAT", "1.23")
//    val fieldData4 = FieldData("d1__s2", "DOUBLE", "12.3")
//    val fieldData5 = FieldData("d1__s2", "BOOLEAN", "true")
//    Assert.assertEquals(123, ConvertLong.toSqlData(fieldData1))
//    Assert.assertEquals("123".toLong, ConvertLong.toSqlData(fieldData2))
//    Assert.assertEquals("1.23".toFloat, ConvertLong.toSqlData(fieldData3))
//    Assert.assertEquals("12.3".toDouble, ConvertLong.toSqlData(fieldData4))
//    Assert.assertEquals("true".toBoolean, ConvertLong.toSqlData(fieldData5))
//  }
//}
