package cn.edu.tsinghua.tsfile

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

private[tsfile] class DefaultSource extends RelationProvider with DataSourceRegister {
  private final val logger = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = "tsfile"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    //All the key value pairs are passed to the RelationProvider.createRelation method
    // in the parameters argument.

    val tsfileOptions = new TSFileOptions(parameters)

    if (tsfileOptions.url == null || tsfileOptions.sql == null) {
      sys.error("TSFile node or sql not specified")
    }
    new TSFileRelation(tsfileOptions)(sqlContext.sparkSession)

  }
}