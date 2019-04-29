package com.atguigu.course

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 教程中 6-10
// 需求：计算每个传感器 15s 窗口中的温度最小值
object ReduceFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorData = env.addSource(new SensorSource)

    val minTempPerWindow = sensorData
      .map(r => (r.id, r.temperature))
      // 先 keyBy
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    minTempPerWindow.print()

    env.execute()
  }
}
