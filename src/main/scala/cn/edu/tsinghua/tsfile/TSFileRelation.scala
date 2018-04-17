package cn.edu.tsinghua.tsfile

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.slf4j.LoggerFactory
import cn.edu.tsinghua.tsfile.qp.common.SQLConstant

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by qjl on 16-8-25.
  */
private case class TSFilePartitioningInfo(
                                           start: Long,
                                           end: Long,
                                           numPartitions: Int)

private object TSFileRelation {

  private final val logger = LoggerFactory.getLogger(classOf[TSFileRelation])

  def getPartitions(partitionInfo: TSFilePartitioningInfo): Array[Partition] = {
    if (partitionInfo == null || partitionInfo.numPartitions <= 1 ||
      partitionInfo.start == partitionInfo.end) {
      return Array[Partition](TSFilePartition(null, 0, 0L, 0L))
    }
    val start = partitionInfo.start
    val end = partitionInfo.end

    //if start <= end , can not partition
    require (start <= end,
      "Operation not allowed: the start time is larger than end time " +
        s"time start: $start; end: $end")

    //numPartitions needs to be less and equal than (end - start)
    val numPartitions =
      if ((end - start) >= partitionInfo.numPartitions) {
        partitionInfo.numPartitions
      } else {
        logger.warn("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${end - start}; Input number of " +
          s"partitions: ${partitionInfo.numPartitions}; Lower bound: $start; " +
          s"Upper bound: $end.")
        end - start
      }

    var partitions = new ArrayBuffer[Partition]()

    val length: Long = (end - start) / numPartitions + 1
    var i: Int = 0
    var currentValue: Long = start
    while (i < numPartitions) {
      var where = s""
      if(i == 0){

        where = s"${SQLConstant.RESERVED_TIME} >= $currentValue and ${SQLConstant.RESERVED_TIME} <= ${currentValue + length}"
        partitions += TSFilePartition(where, i, currentValue, currentValue + length)
      }
      else {
        where = s"${SQLConstant.RESERVED_TIME} > $currentValue and ${SQLConstant.RESERVED_TIME} <= ${currentValue + length}"
        partitions += TSFilePartition(where, i, currentValue+1, currentValue + length)
      }

      i = i + 1
      currentValue += length
    }
    partitions.toArray
  }
}

class TSFileRelation protected[tsfile](val options: TSFileOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  private final val logger = LoggerFactory.getLogger(classOf[TSFileRelation])

  override def schema: StructType = {
    Converter.toSparkSchema(options)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val start: Long = options.lowerBound.toLong
    val end: Long = options.upperBound.toLong
    val numPartition = options.numPartition.toInt

    val partitionInfo = TSFilePartitioningInfo(start, end, numPartition)

    val parts = TSFileRelation.getPartitions(partitionInfo)

    new TSFileRDD(sparkSession.sparkContext,
      options,
      schema,
      requiredColumns,
      filters,
      parts).asInstanceOf[RDD[Row]]
  }
}
