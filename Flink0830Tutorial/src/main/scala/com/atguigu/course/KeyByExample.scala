package com.atguigu.course

import org.apache.flink.streaming.api.scala._

// keyBy需要进行shuffle操作，宽依赖
object KeyByExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // keyBy的例子
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)
    keyedStream.print()

    env.execute()
  }
}
