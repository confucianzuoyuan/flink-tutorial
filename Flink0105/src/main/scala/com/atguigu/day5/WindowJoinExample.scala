package com.atguigu.day5

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input1 = env
      .fromElements(
        ("a", 1, 1000L),
        ("a", 2, 2000L),
        ("b", 1, 3000L),
        ("b", 2, 4000L),
        ("a", 3, 15000L)
      )
      .assignAscendingTimestamps(_._3)

    val input2 = env
      .fromElements(
        ("a", 10, 1000L),
        ("a", 20, 2000L),
        ("b", 10, 3000L),
        ("b", 20, 4000L),
        ("a", 30, 15000L)
      )
      .assignAscendingTimestamps(_._3)

    input1
      .join(input2)
      // on input1._1 = intpu2._1
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new MyJoin)
      .print()

    env.execute()
  }

  // 分流开窗口以后，属于同一个窗口的input1中的元素和input2中的元素做笛卡尔积
  // 相同的key，且是相同的窗口，中的元素做笛卡尔积
  class MyJoin extends JoinFunction[(String, Int, Long), (String, Int, Long), String] {
    override def join(first: (String, Int, Long), second: (String, Int, Long)): String = {
      first + " =====> " + second
    }
  }
}