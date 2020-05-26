package com.atguigu.day6

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink
import org.apache.flink.util.Collector

object FlatMapWithStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      // 第一个参数是输出的元素的类型，第二个参数是状态变量的类型
      .flatMapWithState[(String, Double, Double), Double] {
        case (in: SensorReading, None) => { // 匹配第一个元素
          (List.empty, Some(in.temperature))
        }
        case (r: SensorReading, lastTemp: Some[Double]) => {
            val tempDiff = (r.temperature - lastTemp.get).abs
            if (tempDiff > 1.7) {
              (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
            } else {
              (List.empty, Some(r.temperature))
            }
        }
      }

    stream.print()
    env.execute()
  }


}