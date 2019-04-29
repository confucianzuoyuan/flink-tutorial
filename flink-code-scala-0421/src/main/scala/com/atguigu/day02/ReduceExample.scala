package com.atguigu.day02

import com.atguigu.day02.util.SensorSource
import org.apache.flink.streaming.api.scala._

object ReduceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(r => r.id.equals("sensor_1"))

    stream
      .map(r => (r.id, r.temperature))
      .keyBy(r => r._1)
      .reduce((r1, r2) => {
        (r1._1, r1._2.max(r2._2))
      })
      .print()

    env.execute()
  }
}