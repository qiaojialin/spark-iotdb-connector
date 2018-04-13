package com.corp.spark.tsfile

import java.sql.{Connection, DatabaseMetaData, DriverManager}
import java.util

import query.analyzer.{Analyse, TSFileQuery}
import query.analyzer.operator.{BasicOperator, FilterOperator}
import scala.collection.JavaConversions._
import com.corp.tsfile.file.metadata.enums.TSDataType
import com.corp.tsfile.qp.constant.SQLConstant
import com.corp.tsfile.read.readSupport.ColumnInfo
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
/**
  * Created by qjl on 16-11-3.
  */

class Converter

object Converter {
  private final val logger = LoggerFactory.getLogger(classOf[Converter])

  def toSqlData(field: StructField, value:String): Any ={
    if (value == null) return null
    val r = field.dataType match {
      case BooleanType => java.lang.Boolean.valueOf(value)
      case IntegerType => value.toInt
      case LongType => value.toLong
      case FloatType => value.toFloat
      case DoubleType => value.toDouble
      case StringType => value
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
    r
  }

  def toSqlColumn(columnName: String): String ={
    if (columnName.contentEquals("Timestamp")){
      "time"
    }
    else if(columnName.contentEquals("delta_object")){
      "delta_object"
    }else{
      val t = columnName.split('.')
      t(t.length-1)
    }
  }

  def toSparkDataType(strDataType :String): TSDataType ={
    if (strDataType.contentEquals(TSDataType.INT32.toString)){
      return TSDataType.INT32
    }
    else if (strDataType.contentEquals(TSDataType.INT64.toString)){
      return TSDataType.INT64
    }
    else if (strDataType.contentEquals(TSDataType.BOOLEAN.toString)){
      return TSDataType.BOOLEAN
    }
    else if (strDataType.contentEquals(TSDataType.FLOAT.toString)){
      return TSDataType.FLOAT
    }
    else if (strDataType.contentEquals(TSDataType.DOUBLE.toString)){
      return TSDataType.DOUBLE
    }
    else if (strDataType.contentEquals(TSDataType.ENUMS.toString)){
      return TSDataType.ENUMS
    }
    else if (strDataType.contentEquals(TSDataType.FIXED_LEN_BYTE_ARRAY.toString)){
      return TSDataType.FIXED_LEN_BYTE_ARRAY
    }
    else{
      throw new UnsupportedOperationException(s"Unsupported type $strDataType")
    }
  }

  def toSparkSchema(options: TSFileOptions) : StructType = {

    val url = options.url
    val delta_object = options.delta_object
    Class.forName("com.corp.tsfile.jdbc.TsfileDriver")
    val sqlConn: Connection = DriverManager.getConnection(url, "root", "root")
    val databaseMetaData:DatabaseMetaData= sqlConn.getMetaData
    val resultSet = databaseMetaData.getColumns(null, null, delta_object, null)
    sqlConn.close()
    val tsfileSchema: util.ArrayList[ColumnInfo] = new util.ArrayList[ColumnInfo]()
    while (resultSet.next) {
//      System.out.println(String.format("column %s, type %s", resultSet.getString("COLUMN_NAME"), resultSet.getString("COLUMN_TYPE")))
      tsfileSchema.add(new ColumnInfo(resultSet.getString("COLUMN_NAME"), toSparkDataType(resultSet.getString("COLUMN_TYPE"))))
    }

    val fields = new ListBuffer[StructField]()
    fields += StructField(SQLConstant.RESERVED_TIME, LongType, nullable = true)
    fields += StructField(SQLConstant.RESERVED_DELTA_OBJECT, StringType, nullable = true)

    val unionList = new util.ArrayList[ColumnInfo]()

    tsfileSchema.foreach((sensor: ColumnInfo) => {
      //make field union
      if(!unionList.contains(sensor)) {
        unionList.add(sensor)
        fields += StructField(sensor.getName, sensor.getDataType match {
          case TSDataType.BOOLEAN => BooleanType
          case TSDataType.INT32 => IntegerType
          case TSDataType.INT64 => LongType
          case TSDataType.FLOAT => FloatType
          case TSDataType.DOUBLE => DoubleType
          case TSDataType.FIXED_LEN_BYTE_ARRAY => StringType
          case TSDataType.ENUMS => StringType
          case other => throw new UnsupportedOperationException(s"Unsupported type $other")
          // TODO: INT96, BIGDECIMAL
        }, nullable = true)
      }
    })

    StructType(fields.toList)
  }
}
