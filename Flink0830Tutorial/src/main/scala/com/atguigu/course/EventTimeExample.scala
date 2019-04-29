package com.atguigu.course

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeExample {
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
      // 在source之后马上抽取时间戳和插入水位线
      .assignTimestampsAndWatermarks(
        // 最大延迟时间是 5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          // 时间戳一定要 etl 成毫秒单位
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new WatermarkProcessFunction)
      .print()

    env.execute()

  }

  class WatermarkProcessFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口中共有： " + elements.size.toString + " 条数据")
    }
  }
}
