package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val avgTemp = new AvgTemp()

    // table api
    val table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature" as "temp")

    table
        .groupBy($"id")
        .aggregate(avgTemp($"temp") as "avgTemp")
        .select($"id", $"avgTemp")
        .toRetractStream[Row]
//        .print()

    // sql 写法
    tEnv.createTemporaryView("sensor", table)

    tEnv.registerFunction("avgTemp", avgTemp)

    tEnv.sqlQuery(
      """
        |SELECT
        | id, avgTemp(temp)
        | FROM
        | sensor
        | GROUP BY id""".stripMargin
    )
        .toRetractStream[Row].print()


    env.execute()
  }

  // 累加器的类型
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  // 第一个泛型是温度值的类型
  // 第二个泛型是累加器的类型
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    // 创建累加器
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    // 累加规则
    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }

    override def getValue(accumulator: AvgTempAcc): Double = {
      accumulator.sum / accumulator.count
    }
  }
}