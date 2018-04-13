//package com.corp.spark.tsfile
//
//import com.google.gson.Gson
//import org.junit.{Assert, Before, Test}
//import org.scalatest.junit.JUnitSuite
//
///**
//  * Created by qjl on 16-9-3.
//  */
//class TSFileClientTest extends JUnitSuite {
//
//  private var tableName : String = _
//  private val path = getClass.getClassLoader.getResource("test.tsfile").getPath
//
//  @Before
//  def before(): Unit = {
//    tableName = System.currentTimeMillis().toString
//    val tableInfo = TableInfo(tableName, path)
//    val gson = new Gson()
//    val jsonObject = gson.toJson(tableInfo)
//    val urlTable = "http://localhost:8080/tsfile-rest/api/table"
//    val response = TSFileClient.put(urlTable, jsonObject)
//  }
//
//  @Test
//  def get(): Unit = {
//    val url = "http://localhost:8080/tsfile-rest/api/table/" + tableName
//    val actual = TSFileClient.get(url)
//
//    val expected =
//      "{\"type\":\"tableResponde\",\"message\":\"Success to get the table\"," +
//        "\"statusCode\":1,\"tableInfo\":{\"tableName\":\"" +
//        tableName + "\",\"tablePath\":\"" + path + "\"}}"
//
//    Assert.assertEquals(expected, actual)
//  }
//
//  @Test
//  def put(): Unit = {
//    tableName = System.currentTimeMillis().toString
//    val tableInfo = TableInfo(tableName, path)
//    val gson = new Gson()
//    val jsonObject = gson.toJson(tableInfo)
//    val urlTable = "http://localhost:8080/tsfile-rest/api/table"
//    val actual = TSFileClient.put(urlTable, jsonObject)
//
//    val expected = "{\"type\":\"tableResponde\",\"message\":\"Success to put the table\"," +
//      "\"statusCode\":1,\"tableInfo\":{\"tableName\":\"" +
//      tableName + "\",\"tablePath\":\"" + path + "\"}}"
//
//    Assert.assertEquals(expected, actual)
//  }
//
//  @Test
//  def post(): Unit = {
//    val url = "http://localhost:8080/tsfile-rest/api/tableData"
//    val request = new TableDataRequest(tableName, path, "d1,s1", "null", "null", "null")
//    val gson = new Gson
//    val str = gson.toJson(request)
//    val responde = TSFileClient.post(url, str)
//    val actual = gson.fromJson(responde, classOf[TableDataResponde])
//
//    val expected = "Success to get the data of table"
//
//    Assert.assertEquals(expected, actual.message)
//  }
//
//}
