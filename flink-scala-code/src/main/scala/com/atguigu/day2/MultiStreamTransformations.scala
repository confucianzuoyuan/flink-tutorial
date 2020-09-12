package com.atguigu.day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// freeBSD: Bill Joy: bsd, vi, tcpip, sun公司 -> MacOS
// Linux
object MultiStreamTransformations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val keyedTempStream = env.addSource(new SensorSource).keyBy(r => r.id)

    val smokeLevelStream = env.fromElements("LOW", "HIGH").setParallelism(1)

    keyedTempStream
      .connect(smokeLevelStream.broadcast)
      .flatMap(new RaiseAlertFlatMap)
      .print()

    env.execute()
  }

  class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, String, Alert] {
    private var smokeLevel: String = "LOW"

    override def flatMap1(value: SensorReading, out: Collector[Alert]): Unit = {
      if (smokeLevel == "HIGH" && value.temperature > -10000.0) {
        out.collect(Alert(value.toString, value.timestamp))
      }
    }

    override def flatMap2(value: String, out: Collector[Alert]): Unit = {
      smokeLevel = value
    }
  }
}