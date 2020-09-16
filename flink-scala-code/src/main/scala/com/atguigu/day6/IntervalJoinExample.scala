package com.atguigu.day6

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /*
    A.intervalJoin(B).between(lowerBound, upperBound)
    B.intervalJoin(A).between(-upperBound, -lowerBound)
     */

    val stream1 = env
      .fromElements(
        ("user_1", 10 * 60 * 1000L, "click"),
        ("user_1", 16 * 60 * 1000L, "click")
      )
      .assignAscendingTimestamps(_._2)
      .keyBy(r => r._1)

    val stream2 = env
      .fromElements(
        ("user_1", 5 * 60 * 1000L, "browse"),
        ("user_1", 6 * 60 * 1000L, "browse")
      )
      .assignAscendingTimestamps(_._2)
      .keyBy(r => r._1)

    stream1
      .intervalJoin(stream2)
      .between(Time.minutes(-10), Time.minutes(0))
      .process(new ProcessJoinFunction[(String, Long, String), (String, Long, String), String] {
        override def processElement(in1: (String, Long, String), in2: (String, Long, String), context: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, collector: Collector[String]): Unit = {
          collector.collect(in1 + " => " + in2)
        }
      })
      .print()

    stream2
      .intervalJoin(stream1)
      .between(Time.minutes(0), Time.minutes(10))
      .process(new ProcessJoinFunction[(String, Long, String), (String, Long, String), String] {
        override def processElement(in1: (String, Long, String), in2: (String, Long, String), context: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, collector: Collector[String]): Unit = {
          collector.collect(in1 + " => " + in2)
        }
      })
      .print()

    env.execute()
  }
}