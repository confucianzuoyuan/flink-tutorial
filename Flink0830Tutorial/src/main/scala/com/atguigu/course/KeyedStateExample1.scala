package com.atguigu.course

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedStateExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .flatMap(new TemperatureAlertFunction(1.7))
      .print()

    env.execute()
  }

  class TemperatureAlertFunction(val threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
    private var lastTempState : ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      lastTempState = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("lastTempState", Types.of[Double])
      )
    }

    // 等价于
//    lazy val lastTempState = getRuntimeContext.getState(
//      new ValueStateDescriptor[Double]("lastTempState", Types.of[Double])
//    )

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp = lastTempState.value()
      val tempDiff = (value.temperature - lastTemp).abs
      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, lastTemp))
      }
      this.lastTempState.update(value.temperature)
    }
  }
}
