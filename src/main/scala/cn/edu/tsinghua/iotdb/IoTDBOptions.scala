package cn.edu.tsinghua.iotdb

/**
  * Created by qjl on 16-11-4.
  */
class IoTDBOptions(
                    @transient private val parameters: Map[String, String])
  extends Serializable {

  val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))

  val user = parameters.getOrElse("user", "root")

  val password = parameters.getOrElse("password", "root")

  val sql = parameters.getOrElse("sql", null)

  // deprecated:

  val delta_object = parameters.getOrElse("delta_object", null)

  val TsIndex: Array[String] = parameters.getOrElse("TsIndex", "").split(",") // start from 1

  val TsName: Array[String] = parameters.getOrElse("TsName", "").split(",")

  val TsType: Array[String] = parameters.getOrElse("TsType", "").split(",")

  val TsEnc: Array[String] = parameters.getOrElse("TsEnc", "").split(",")

  val numPartition = parameters.getOrElse("numPartition", "1")

  val lowerBound = parameters.getOrElse("lowerBound", "0")

  val upperBound = parameters.getOrElse("upperBound", "0")

  def get(name: String): Unit = {

  }
}
