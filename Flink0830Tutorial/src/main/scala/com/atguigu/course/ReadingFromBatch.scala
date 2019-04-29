package com.atguigu.course

import org.apache.flink.streaming.api.scala._

// 对应教程的5.7.1
object ReadingFromBatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val readings = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718199, 15.402984393403084),
      SensorReading("sensor_7", 1547718199, 6.720945201171228),
      SensorReading("sensor_10", 1547718199, 38.101067604893444)
    ))
    readings.print()

    env.execute()
  }
}
