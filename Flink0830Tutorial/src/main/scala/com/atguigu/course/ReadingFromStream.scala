package com.atguigu.course

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

object ReadingFromStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    // 匿名函数来实现map算子
    val mapStreamWithLambda: DataStream[String] = stream.map(r => r.id)
    mapStreamWithLambda.print()


    // 使用接口的方式来实现map算子
    val mapStreamWithInterface: DataStream[String] = stream.map(new MyMapFunction)
    mapStreamWithInterface.print()

    // 使用匿名函数来实现filter算子
    val filterStreamWithLambda: DataStream[SensorReading] = stream.filter(r => r.temperature > 0)
    filterStreamWithLambda.print()

    // 使用接口的方式来实现filter算子
    val filterStreamWithInterface: DataStream[SensorReading] = stream.filter(new MyFilterFunction)
    filterStreamWithInterface.print()

    // one-to-one
//    val streamExample = env
//      .addSource(new SensorSource).setParallelism(1)
//      .map(r => r.temperature).setParallelism(1)
//      .filter(r => r > 20).setParallelism(1)
//      .flatMap().setParallelism(1)

    env.execute()
  }

  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(t: SensorReading): String = t.id
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.temperature > 0
  }
}
