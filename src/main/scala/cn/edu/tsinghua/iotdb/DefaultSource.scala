package cn.edu.tsinghua.iotdb

import java.sql.{Connection, DriverManager, Statement}

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

private[iotdb] class DefaultSource extends RelationProvider
  with DataSourceRegister
  with SchemaRelationProvider
  with CreatableRelationProvider {
  private final val logger = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = "tsfile"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    createRelation(sqlContext, parameters, null)

  }

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String], schema: StructType): BaseRelation = {

    val iotdbOptions = new IoTDBOptions(parameters)

    new IoTDBRelation(iotdbOptions, schema)(sqlContext.sparkSession)

  }

  override def createRelation(
                               sqlContext: SQLContext, mode: SaveMode,
                               parameters: Map[String, String], data: DataFrame): BaseRelation = {

    saveAsIoTDBData(data, parameters)

    createRelation(sqlContext, parameters, data.schema)

  }

  def saveAsIoTDBData(data: DataFrame, parameters: Map[String, String]) = {

    val path = checkPath(parameters)

    val options = new IoTDBOptions(parameters)

    Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
    val sqlConn: Connection = DriverManager.getConnection(options.url, options.user, options.password)
    val sqlStatement: Statement = sqlConn.createStatement()

    sqlStatement.addBatch("SET STORAGE GROUP TO " + path)

    val TsIndex = options.TsIndex
    val TsName = options.TsName
    val TsType = options.TsType
    val TsEnc = options.TsEnc

    for (i <- 0 until TsIndex.length) {
      sqlStatement.addBatch("CREATE TIMESERIES " + TsName(i) + " WITH DATATYPE=" + TsType(i) + ", ENCODING=" + TsEnc(i))
    }

    data.rdd.collect().foreach(row =>
      for (i <- 0 until TsIndex.length) {
        val pos = TsIndex(i).toInt
        if (row(pos) != null) {
          val tsArray = TsName(i).split("\\.")
          val buf = new StringBuilder(tsArray(0))
          for (j <- 1 until tsArray.size - 1) {
            buf.append(".").append(tsArray(j))
          }
          if(TsType(i).toUpperCase().equals("TEXT")) {
            sqlStatement.addBatch("insert into " + buf.toString()
              + "(timestamp," + tsArray(tsArray.size - 1) + ") values(" + row(0) + ",\"" + row(pos) + "\")")
          }
          else {
            sqlStatement.addBatch("insert into " + buf.toString()
              + "(timestamp," + tsArray(tsArray.size - 1) + ") values(" + row(0) + "," + row(pos) + ")")
          }

        }
      }
    )

    sqlStatement.executeBatch()

  }


  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("delta_object", sys.error("'delta_object' must be specified for saving IoTDB data."))
  }


}