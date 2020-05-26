package com.atguigu.day5

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 每个10s做一次检查点操作
    env.enableCheckpointing(10000L)
    // windows: file:\\ + 绝对路径
    env.setStateBackend(new FsStateBackend("file:///Users/yuanzuo/Desktop/flink-tutorial/Flink0105/src/main/resources/checkpoint"))

    // env.setStateBackend(new FsStateBackend("file:///").asInstanceOf[StateBackend])

    val stream = env
      .addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new Keyed)

    stream.print()

    env.execute()
  }

  class Keyed extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val listState = getRuntimeContext.getListState(
      new ListStateDescriptor[SensorReading]("list-state", Types.of[SensorReading])
    )

    lazy val timer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value) // 将value添加到列表状态中
      if (timer.value() == 0L) {
        val ts = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timer.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 不能直接对列表状态变量进行计数
      val readings: ListBuffer[SensorReading] = new ListBuffer()
      // 隐式类型转换必须导入
      import scala.collection.JavaConversions._
      for (r <- listState.get) {
        readings += r
      }
      out.collect("当前时刻列表状态变量里面共有 " + readings.size + "条数据！")
      timer.clear()
    }
  }
}