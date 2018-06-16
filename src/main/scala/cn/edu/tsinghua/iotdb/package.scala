package cn.edu.tsinghua

/**
  * Created by qjl on 16-9-5.
  */
import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object iotdb {

  val myPackage = "cn.edu.tsinghua.tsfile"

  /**
    * Adds a method, `iotdb`, to DataFrameReader that allows you to read data from IoTDB using
    * the DataFileReade
    */
  implicit class IoTDBDataFrameReader(reader: DataFrameReader) {
    def iotdb: (Map[String, String]) => DataFrame = reader.format(myPackage).options(_).load()
  }
}