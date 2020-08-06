package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val top2Temp = new Top2Temp()

    // table api
    val table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature")

    table
        .groupBy($"id")
        .flatAggregate(top2Temp($"temperature") as ("temp", "rank"))
        .select($"id", $"temp", $"rank")
        .toRetractStream[Row]
        .print()

    env.execute()
  }

  // 累加器
  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  // 第一个泛型是输出：（温度值，排名）
  // 第二个泛型是累加器
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    // 函数名必须是accumulate
    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // 函数名必须是emitValue，用来发射计算结果
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }
}