package com.corp.spark.tsfile

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

/**
  * Created by qjl on 16-8-25.
  */
class TSFileRelationProvider extends RelationProvider with DataSourceRegister{

  private final val logger = LoggerFactory.getLogger(classOf[TSFileRelationProvider])

  override def shortName(): String = "tsfile"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    val tsfileOptions = new TSFileOptions(parameters)

    if (tsfileOptions.url == null || tsfileOptions.delta_object == null) {
      sys.error("TSFile node and path not specified")
    }
    new TSFileRelation(tsfileOptions)(sqlContext.sparkSession)

  }
}