### 代码练习（以分组滚动窗口为例）

我们可以综合学习过的内容，用一段完整的代码实现一个具体的需求。例如，可以开一个滚动窗口，统计10秒内出现的每个sensor的个数。

代码如下：

**scala version**

```scala
object TumblingWindowTempCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource).filter(r => r.id.equals("sensor_1"))

    val table = tableEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature", $"pt".proctime())

    // table api
    val tableResult = table
      .window(Tumble over 10.seconds() on $"pt" as $"w")
      .groupBy($"id", $"w") // .keyBy(r => r.id).timeWindow(Time.seconds(10))
      .select($"id", $"id".count())

    tableEnv.toRetractStream[Row](tableResult).print()

    // sql
    tableEnv.createTemporaryView("sensor", stream, $"id", $"timestamp" as "ts", $"temperature", $"pt".proctime())

    val sqlResult = tableEnv
      .sqlQuery("SELECT id, count(id), TUMBLE_START(pt, INTERVAL '10' SECOND), TUMBLE_END(pt, INTERVAL '10' SECOND) FROM sensor GROUP BY id, TUMBLE(pt, INTERVAL '10' SECOND)")

    tableEnv.toRetractStream[Row](sqlResult).print()

    env.execute()
  }
}
```

**java version**
