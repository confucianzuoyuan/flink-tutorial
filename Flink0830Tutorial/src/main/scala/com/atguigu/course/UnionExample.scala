package com.atguigu.course

import org.apache.flink.streaming.api.scala._

object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1 = env.fromElements(1,4)
    val stream2 = env.fromElements(2,5)
    val stream3 = env.fromElements(3,6)
    // 先进先出（FIFO）
    stream1.union(stream2, stream3).print()
    env.execute()
  }
}
