./spark-shell --jars /home/rl/tsfile-0.5.0-SNAPSHOT.jar,/home/rl/iotdb-jdbc-0.5.0-SNAPSHOT.jar,/home/rl/iotdb-spark-connector-0.5.0.jar

val df = spark.read.format("cn.edu.tsinghua.tsfile").option("url","jdbc:tsfile://127.0.0.1:6667/").option("sql","select * from root").option("delta_object","as").load

df.printSchema()

df.show()


======

old:

启动spark 添加jar包
/usr/local/spark/bin/spark-shell --jars /home/hadoop/git/tsfile/delta-spark/target/delta.spark-1.0-SNAPSHOT.jar

创建long表
sql("create temporary view tsfile using com.corp.spark.tsfile options(path = \"/home/hadoop/git/tsfile/tsfile-rest/write.tsfile\")")

展示所有数据
sql("select * from tsfile").show()

投影
sql("select timestamp, device_1__sensor_cpu_50, device_1__sensor_ms_50 from tsfile").show()
sql("select device_1__sensor_cpu_50, device_1__sensor_ms_50 from tsfile").show()

过滤时间列
sql("select * from tsfile where timestamp < 1463369806250 and timestamp > 1463369800000").show()

过滤时间列和值列
sql("select * from tsfile where (timestamp < 1463369806250 and timestamp > 1463369800000) and device_1__sensor_cpu_50 < 70 ").show()

聚合操作
sql("select count(device_1__sensor_cpu_1) from tsfile").show()
sql("select max(device_1__sensor_cpu_1) from tsfile").show()





创建union表
sql("create temporary view tsfile using com.corp.spark.tsfile options(path = \"/home/hadoop/git/tsfile/tsfile-rest/write.tsfile\", union=\"true\")")
