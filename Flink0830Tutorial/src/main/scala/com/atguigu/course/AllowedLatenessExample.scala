package com.atguigu.course

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowedLatenessExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 9999, '\n')
    stream
      .map(r => {
        val arr = r.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      // 最大延迟时间是 5s
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      // 滚动窗口大小 5s
      .timeWindow(Time.seconds(5))
      // 窗口等待迟到元素 5s
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)
      .print()

    env.execute()
  }

  class UpdatingWindowCountFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      val cnt = elements.size

      // 初始化一个windowState，只对本窗口可见的状态变量
      // 初始值是 null
      val isUpdate = context.windowState.getState(
        new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean])
      )

      // 第一次求值的条件： 水位线超过窗口结束时间
      if (!isUpdate.value()) {
        out.collect("第一次窗口求值，总数为： " + cnt.toString)
        isUpdate.update(true)
      } else {
        // 迟到元素到了以后会触发的条件分支
        out.collect("迟到元素来了！总数为： " + cnt.toString)
      }
    }
  }
}
