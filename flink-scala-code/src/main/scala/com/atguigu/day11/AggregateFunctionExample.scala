package com.atguigu.day11

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    val avgTemp = new AvgTemp()

    // sql
    tEnv.createTemporaryView("sensor", stream)
    tEnv.registerFunction("avgTemp", avgTemp)
    val sqlResult = tEnv.sqlQuery("SELECT id, avgTemp(temperature) FROM sensor GROUP BY id")
    tEnv.toRetractStream[Row](sqlResult).print()

    // table api
    val table = tEnv.fromDataStream(stream)
    val tableResult = table.groupBy($"id").aggregate(avgTemp($"temperature") as "avgTemp").select($"id", $"avgTemp")
    tEnv.toRetractStream[Row](tableResult).print()

    env.execute()
  }

  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count

    def accumulate(acc: AvgTempAcc, in: Double): Unit = {
      acc.sum += in
      acc.count += 1
    }
  }
}