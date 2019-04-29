package com.atguigu.day11

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    val table = tEnv.fromDataStream(stream)

    val top2Temp = new Top2Temp()

    val resultTable = table
      .groupBy($"id")
      .flatAggregate(top2Temp($"temperature") as ("temp", "rank"))
      .select($"id", $"temp", $"rank")

    tEnv.toRetractStream[Row](resultTable).print()

    env.execute()
  }

  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    def accumulate(acc: Top2TempAcc, in: Double): Unit = {
      if (in > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = in
      } else if (in > acc.secondHighestTemp) {
        acc.secondHighestTemp = in
      }
    }

    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }
}