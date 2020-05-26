package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    val setttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, setttings)

    val table = tableEnv.fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature as 'temp)

    val top2Temp = new Top2Temp

    table
        .groupBy('id)
        .flatAggregate(top2Temp('temp) as ('temp, 'rank))
        .select('id, 'temp, 'rank)
        .toRetractStream[Row]
        .print()

    env.execute()
  }

  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // (Double, Int) => (温度，排名)
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }


}