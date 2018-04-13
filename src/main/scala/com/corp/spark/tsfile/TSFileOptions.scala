package com.corp.spark.tsfile

/**
  * Created by qjl on 16-11-4.
  */
class TSFileOptions(
                     @transient private val parameters: Map[String, String])
  extends Serializable {

  val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))

  val delta_object = parameters.getOrElse("delta_object", sys.error("Option 'path' not specified"))

  val numPartition = parameters.getOrElse("numPartition", "1")

  val lowerBound = parameters.getOrElse("lowerBound", "0")

  val upperBound = parameters.getOrElse("upperBound", "0")

  def get(name: String): Unit = {

  }
}
