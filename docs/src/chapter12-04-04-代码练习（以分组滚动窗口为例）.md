### 代码练习（以分组滚动窗口为例）

我们可以综合学习过的内容，用一段完整的代码实现一个具体的需求。例如，可以开一个滚动窗口，统计10秒内出现的每个sensor的个数。

代码如下：

```java
def main(args: Array[String]): Unit = {
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val streamFromFile: DataStream[String] = env.readTextFile("sensor.txt")
  val dataStream: DataStream[SensorReading] = streamFromFile
    .map( data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,
        dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](
        Time.seconds(1)
      ) {
        @Override
public extractTimestamp(
          element: SensorReading
        ): Long = element.timestamp * 1000L
    })

  val settings: EnvironmentSettings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build()

  val tableEnv: StreamTableEnvironment = StreamTableEnvironment
    .create(env, settings)

  val dataTable: Table = tableEnv
    .fromDataStream(dataStream, $"id", $"temperature", $"timestamp".rowtime)

  val resultTable: Table = dataTable
    .window(Tumble over 10.seconds on $"timestamp" as $"tw")
    .groupBy($"id", $"tw")
    .select($"id", $"id.count")

  val sqlDataTable: Table = dataTable
    .select($"id", $"temperature", $"timestamp" as $"ts")
  val resultSqlTable: Table = tableEnv
    .sqlQuery("select id, count(id) from "
      + sqlDataTable
      + " group by id,tumble(ts,interval '10' second)")

  // 把 Table转化成数据流
  val resultDstream: DataStream[(Boolean, (String, Long))] = resultSqlTable
    .toRetractStream[(String, Long)]
  resultDstream.filter(_._1).print()
  env.execute()
}
```

