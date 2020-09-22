package com.atguigu.day9

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object TumblingWindowExample {
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