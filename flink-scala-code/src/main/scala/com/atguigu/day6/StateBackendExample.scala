package com.atguigu.day6

import com.atguigu.day2.SensorSource
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._

object StateBackendExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new FsStateBackend("file:///home/zuoyuan/flink-tutorial/flink-scala-code/src/main/resources/checkpoints"))
    env.enableCheckpointing(10 * 1000L)

    val stream = env.addSource(new SensorSource)

    stream.print()

    env.execute()
  }
}