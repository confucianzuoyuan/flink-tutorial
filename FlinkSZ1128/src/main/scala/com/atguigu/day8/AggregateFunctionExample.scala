package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object AggregateFunctionExample {
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

    // 实例化udf函数
    val avgTemp = new AvgTemp

    table
      .groupBy('id)
      .aggregate(avgTemp('temp) as 'avgTemp)
      .select('id, 'avgTemp)
      .toRetractStream[Row]
//      .print()

    // 使用sql的方式
    // 创建临时表
    tableEnv.createTemporaryView("t", table)
    // 注册udf函数
    tableEnv.registerFunction("avgTemp", avgTemp)

    tableEnv
        .sqlQuery(
          """
            |SELECT id, avgTemp(temp) FROM t GROUP BY id""".stripMargin)
        .toRetractStream[Row]
        .print()

    env.execute()
  }

  // 累加器的类型
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    // 创建累加器
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    // 累加规则
    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }

    // 获取结果
    override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count
  }
}