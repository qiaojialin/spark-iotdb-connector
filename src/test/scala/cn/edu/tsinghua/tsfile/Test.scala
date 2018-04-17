package cn.edu.tsinghua.tsfile
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.{After, Assert, Before, Test}
import org.scalatest.junit.JUnitSuite
import cn.edu.tsinghua.tsfile.qp.common.SQLConstant

class TSFileSuit extends JUnitSuite {
  private val testFile = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val csvPath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.csv"
  private val tsfilePath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val errorPath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/errortest.tsfile"
  private var sqlContext: SQLContext = _
  private var spark: SparkSession = _

  @Before
  def before(): Unit = {
    //sqlContext = new SQLContext(new SparkContext("local[2]", "TSFileSuit"))
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  @After
  def after(): Unit = {
    /*
    if (sqlContext != null) {
      sqlContext.sparkContext.stop()
    }
    */
    if (spark != null) {
      spark.sparkContext.stop()
    }

  }

  @Test
  def showData(): Unit = {
    val df = spark.read.format("cn.edu.tsinghua.tsfile").option("url", "jdbc:tsfile://127.0.0.1:6667/").option("sql", "select * from root").load
    df.printSchema()
    df.show()
  }
}
