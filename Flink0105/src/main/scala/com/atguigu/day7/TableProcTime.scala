package com.atguigu.day7

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object TableProcTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    // 'pt.proctime指定了处理时间是'pt字段，必须放在最后
    val table: Table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature" as "temp", $"pt".proctime)

    // table api 窗口操作
    table
      // 开窗口，并命名为$"w"
        .window(Tumble over 10.seconds on $"pt" as $"w")
      // .keyBy(_.id).timeWindow(Time.seconds(10))
        .groupBy($"id", $"w")
      // .process
        .select($"id", $"id".count)
        .toAppendStream[Row]
        .print()

    env.execute()
  }
}