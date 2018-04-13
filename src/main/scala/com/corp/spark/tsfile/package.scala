package com.corp.spark

/**
  * Created by qjl on 16-9-5.
  */
import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object tsfile {

  val myPackage = "com.corp.spark.tsfile"

  /**
    * Adds a method, `tsfile`, to DataFrameReader that allows you to read tsfile files using
    * the DataFileReade
    */
  implicit class TSFileDataFrameReader(reader: DataFrameReader) {
    def tsfile: (Map[String, String]) => DataFrame = reader.format(myPackage).options(_).load()
  }
}