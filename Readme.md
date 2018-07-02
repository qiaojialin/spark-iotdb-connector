## 版本

The versions required for Spark and Java are as follow:

| Spark Version | Scala Version | Java Version | TsFile |
| ------------- | ------------- | ------------ |------------ |
| `2.0-2.1`        | `2.11`        | `1.8`        | `0.5.0`|


## 安装
mvn clean scala:compile compile install


## maven 依赖

```
    <dependency>
      <groupId>cn.edu.tsinghua</groupId>
      <artifactId>spark-iotdb-connector</artifactId>
      <version>0.5.0</version>
    </dependency>
```


## spark-shell使用方式

```
./spark-shell --jars /home/rl/tsfile-0.5.0-SNAPSHOT.jar,/home/rl/iotdb-jdbc-0.5.0-SNAPSHOT.jar,/home/rl/spark-iotdb-connector-0.5.0.jar

val df = spark.read.format("cn.edu.tsinghua.tsfile").option("url","jdbc:tsfile://127.0.0.1:6667/").option("sql","select * from root").load

df.printSchema()

df.show()
```

## save data from Spark to IoTDB
DataFrame is the connection point when saving data from Spark to IoTDB, and vice versa.
```
./spark-shell --jars /home/rl/tsfile-0.7.0.jar,/home/rl/iotdb-jdbc-0.7.0.jar,/home/rl/spark-iotdb-connector-0.7.0.jar

import org.apache.spark.sql.types._

import org.apache.spark.sql._

val schema = StructType(List(

    StructField("Time", LongType, nullable = false),

    StructField("root.test.sensor1", StringType, nullable = true),

    StructField("root.test.sensor2", DoubleType, nullable = true)

))

val rdd = sc.parallelize(Seq(

    Row(1.toLong, "First Value", 2.2.toDouble),

    Row(2.toLong, "Second Value", 30.toDouble)

))

val sqlContext = new SQLContext(sc)

val df = sqlContext.createDataFrame(rdd, schema)

df.write.format("cn.edu.tsinghua.iotdb").option("url","jdbc:tsfile://127.0.0.1:6667/").option("TsIndex","1,2").option("delta_object","root.test").option("TsName","root.test.sensor1,root.test.sensor2").option("TsType","TEXT, DOUBLE").option("TsEnc","PLAIN, RLE").save

val df2 = spark.read.format("cn.edu.tsinghua.iotdb").option("url","jdbc:tsfile://127.0.0.1:6667/").option("sql","select * from root").load

df2.printSchema

df2.show

```
