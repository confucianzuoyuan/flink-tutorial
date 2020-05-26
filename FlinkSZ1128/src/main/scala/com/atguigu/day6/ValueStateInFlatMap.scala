package com.atguigu.day6

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ValueStateInFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new TemperatureAlert(1.7))

    stream.print()
    env.execute()
  }

  class TemperatureAlert(val diff: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
    private var lastTemp: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      lastTemp = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("last-temp", classOf[Double])
      )
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val last = lastTemp.value()
      val tempDiff = (value.temperature - last).abs // 差值的绝对值
      if (tempDiff > diff) {
        out.collect((value.id, value.temperature,tempDiff))
      }
      lastTemp.update(value.temperature)
    }
  }
}