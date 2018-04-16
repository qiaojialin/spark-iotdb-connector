package cn.edu.tsinghua.tsfile

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, Statement}
import java.util

import cn.edu.tsinghua.iotdb.query.analyzer.{Analyse, TSFileQuery}
import cn.edu.tsinghua.tsfile.qp.common.{BasicOperator, FilterOperator, SQLConstant}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.JavaConversions._

/**
  * Created by qjl on 16-11-3.
  */

//TSFile partition
case class TSFilePartition(where: String, id: Int, start: java.lang.Long, end:java.lang.Long) extends Partition {
  override def index: Int = id
}

object TSFileRDD{

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  def resolveFilter(filter: Filter): String = {
    filter match {
      case f: EqualTo => f.attribute + " = " + f.value.toString

      case f: GreaterThan => f.attribute + " > " + f.value.toString

      case f: GreaterThanOrEqual => f.attribute + " >= " + f.value.toString

      case f: LessThan => f.attribute + " < " + f.value.toString

      case f: LessThanOrEqual => f.attribute + " <= " + f.value.toString

      case f: Or => {
        var result = new String
        if (resolveFilter(f.left) == ""){
          result = resolveFilter(f.right)
        }
        else if(resolveFilter(f.right) == ""){
          result = resolveFilter(f.left)
        }
        else {
          result = "(" + resolveFilter(f.left) + " or " + resolveFilter(f.right) + ")"
        }
        result
      }

      case f: And => {
        var result = new String
        if (resolveFilter(f.left) == ""){
          result = resolveFilter(f.right)
        }
        else if(resolveFilter(f.right) == ""){
          result = resolveFilter(f.left)
        }
        else {
          result = "(" + resolveFilter(f.left) + " and " + resolveFilter(f.right) + ")"
        }
        result
      }

      case _ => ""
    }
  }

  private def toTSFileSql(filter: FilterOperator):String = {
    if(filter == null){
      return null
    }
    if(filter.isInstanceOf[BasicOperator]){
      return filter.asInstanceOf[BasicOperator].getSeriesPath + filter.asInstanceOf[BasicOperator].getTokenSymbol + filter.asInstanceOf[BasicOperator].getSeriesValue
    }
    else {
      if(filter.getTokenIntType == SQLConstant.KW_AND){
        var i = 0
        val where = new StringBuilder
        val children = filter.getChildren
        if(children.size() > 0){
          where.append("(")
        }
        children.foreach(child => {
          if(i == 0){
            where.append(toTSFileSql(child))
          }
          else{
            where.append(" and " + toTSFileSql(child))
          }
          i += 1
        })
        if(children.size() > 0){
          where.append(")")
        }

        where.toString()
      }
      else if(filter.getTokenIntType == SQLConstant.KW_OR){
        var i = 0
        val where = new StringBuilder
        val children = filter.getChildren
        if(children.size() > 0){
          where.append("(")
        }
        children.foreach(child => {
          if(i == 0){
            where.append(toTSFileSql(child))
          }
          else{
            where.append(" or " + toTSFileSql(child))
          }
          i += 1
        })
        if(children.size() > 0){
          where.append(")")
        }

        where.toString()
      }
      else{
        null
      }
    }
  }

}

class TSFileRDD private[tsfile](
                                sc: SparkContext,
                                options: TSFileOptions,
                                schema : StructType,
                                requiredColumns: Array[String],
                                filters: Array[Filter],
                                partitions: Array[Partition])
  extends RDD[Row](sc, Nil) {

  private def analyzeFilter(node: Filter): FilterOperator = {
    var operator: FilterOperator = null
    node match {
      case node: Not =>
        operator = new FilterOperator(SQLConstant.KW_NOT)
        operator.addChildOPerator(analyzeFilter(node.child))
        operator

      case node: And =>
        operator = new FilterOperator(SQLConstant.KW_AND)
        operator.addChildOPerator(analyzeFilter(node.left))
        operator.addChildOPerator(analyzeFilter(node.right))
        operator

      case node: Or =>
        operator = new FilterOperator(SQLConstant.KW_OR)
        operator.addChildOPerator(analyzeFilter(node.left))
        operator.addChildOPerator(analyzeFilter(node.right))
        operator

      case node: EqualTo =>
        operator = new BasicOperator(SQLConstant.EQUAL, node.attribute, node.value.toString)
        operator

      case node: LessThan =>
        operator = new BasicOperator(SQLConstant.LESSTHAN, node.attribute, node.value.toString)
        operator

      case node: LessThanOrEqual =>
        operator = new BasicOperator(SQLConstant.LESSTHANOREQUALTO, node.attribute, node.value.toString)
        operator

      case node: GreaterThan =>
        operator = new BasicOperator(SQLConstant.GREATERTHAN, node.attribute, node.value.toString)
        operator

      case node: GreaterThanOrEqual =>
        operator = new BasicOperator(SQLConstant.GREATERTHANOREQUALTO, node.attribute, node.value.toString)
        operator

      case _ =>
        throw new Exception("unsupported filter:" + node.toString)
    }
  }

  private def queryToSql(tsFileQuery: TSFileQuery, part : TSFilePartition) : String = {
    val select = StringBuilder.newBuilder
    val where = StringBuilder.newBuilder

    val paths = tsFileQuery.getPaths
    var i = 0
    paths.foreach(path => {
      if(i == 0){
        select.append(path)
      }
      else {
        select.append("," + path)
      }
      i += 1
    })
    val timeStr = TSFileRDD.toTSFileSql(tsFileQuery.getTimeFilterOperator)
    val valueStr = TSFileRDD.toTSFileSql(tsFileQuery.getValueFilterOperator)
    if(timeStr != null && valueStr != null){
      where.append(timeStr + " and " + valueStr + " and " + part.where)
    }
    else if(timeStr == null && valueStr != null){
      where.append(valueStr + " and " + part.where)
    }
    else if(timeStr != null && valueStr == null){
      where.append(timeStr + " and " + part.where)
    }
    else {
      where.append(part.where)
    }

    val sql = new StringBuilder
    sql.append("select " + select)
    sql.append(" where " + where)

    sql.toString()


  }

  private def toTSFileSql(part : TSFilePartition): Array[String] = {

    val paths = new util.ArrayList[String]
    requiredColumns.foreach(f => {
      paths.add(f)
    })

    //remove invalid filters
    val validFilters = new util.ArrayList[Filter]()
    filters.foreach {
      case f: EqualTo => validFilters.add(f)
      case f: GreaterThan => validFilters.add(f)
      case f: GreaterThanOrEqual => validFilters.add(f)
      case f: LessThan => validFilters.add(f)
      case f: LessThanOrEqual => validFilters.add(f)
      case f: Or => validFilters.add(f)
      case f: And => validFilters.add(f)
      case f: Not => validFilters.add(f)
      case _ => null
    }

    if(validFilters.isEmpty) {

      //analyse operatorTree to TSFileQuery list
      val analyse = new Analyse()
      val tsfileQuerys = analyse.analyse(null, paths, options.url, options.delta_object, part.start, part.end)

      val sqls = new ArrayBuffer[String]
      tsfileQuerys.foreach(tsq => {
        sqls.append(queryToSql(tsq, part))
      })
      sqls.toArray
    }
    else {
      var filterTree = validFilters.get(0)
      for(i <- 1 until validFilters.length) {
        filterTree = And(filterTree, validFilters.get(i))
      }

      //convert filterTree to FilterOperator
      val operator = analyzeFilter(filterTree)

      //analyse operatorTree to TSFileQuery list
      val analyse = new Analyse()
      val tsfileQuerys = analyse.analyse(operator, paths, options.url, options.delta_object, part.start, part.end)

      val sqls = new ArrayBuffer[String]
      tsfileQuerys.foreach(tsq => {
        sqls.append(queryToSql(tsq, part))
      })
      sqls.toArray
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    var finished = false
    var gotNext = false
    var nextValue: Row = null
    val inputMetrics = context.taskMetrics().inputMetrics

    val part = split.asInstanceOf[TSFilePartition]

    println(">>>>>>>@@@@@@@@@@@@")
    var taskInfo: String = _
    Option(TaskContext.get()).foreach { taskContext => {
      taskContext.addTaskCompletionListener { _ => conn.close()}
      taskInfo = "task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId()
      println(taskInfo)
    }
    }

    Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver")
    val conn: Connection = DriverManager.getConnection(options.url, options.user, options.password)
    val stmt: Statement = conn.createStatement()
    /*
    val metaData = conn.getMetaData

    val delta_objects = ArrayBuffer[String]()
    val resultSet = metaData.getColumns(null, null, null, options.delta_object)
    while (resultSet.next()){
      delta_objects.append(resultSet.getString("DELTA_OBJECT"))
    }
    val sqls = toTSFileSql(part)
    var index = 0
    var sql = sqls(index)
    */

    var rs : ResultSet = stmt.executeQuery(options.sql)
    println(options.sql)
    val prunedSchema = TSFileRDD.pruneSchema(schema, requiredColumns)
    private val rowBuffer = Array.fill[Any](prunedSchema.length)(null)

    def getNext(): Row = {
      if (rs.next()) {
        val fields = new scala.collection.mutable.HashMap[String, String]()
        for(i <- 1 until rs.getMetaData.getColumnCount+1) { // start from 1
          val field = rs.getString(i)
          //fields.put(Converter.toSqlColumn(rs.getMetaData.getColumnName(i)), field)
          fields.put(rs.getMetaData.getColumnName(i), field)
        }

        //index in one required row
        var index = 0
        prunedSchema.foreach((field: StructField) => {
          val r = Converter.toSqlData(field, fields.getOrElse(field.name, null))
          rowBuffer(index) = r
          index += 1
          /*
          if(field.name == SQLConstant.RESERVED_TIME) {
            rowBuffer(index) = rs.getLong(0)
          } else if (field.name == SQLConstant.RESERVED_DELTA_OBJECT) {
            rowBuffer(index) = delta_objects(index)
          } else {
            val r = Converter.toSqlData(field, fields.getOrElse(field.name, null))
            rowBuffer(index) = r
          }
          index += 1
          */
        })
        Row.fromSeq(rowBuffer)
      }
      else{
        finished = true
        null
      }
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
          gotNext = true
        }
      }
      !finished
    }

    override def next(): Row = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }

  override def getPartitions: Array[Partition] = partitions


}