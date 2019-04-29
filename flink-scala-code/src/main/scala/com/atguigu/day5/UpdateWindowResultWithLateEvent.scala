package com.atguigu.day5

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//a 1
//a 2
//a 3
//a 10
//a 1
//a 1
//a 1
//a 15
//a 1
//a 1
//a 1
object UpdateWindowResultWithLateEvent {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(r => {
        val arr = r.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
          })
      )
      .keyBy(r => r._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(5))
      .process(new CountWindow)

    stream.print()

    env.execute()
  }

  class CountWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      // note that here is window state!!! only for current key and current window
      val isUpdate = context.windowState.getState(
        new ValueStateDescriptor[Boolean]("is-update", Types.of[Boolean])
      )

      if (!isUpdate.value()) {
        out.collect("first calculate window result!!!!")
        isUpdate.update(true)
      } else {
        out.collect("update window result!!!!")
      }
    }
  }
}