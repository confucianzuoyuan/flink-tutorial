package com.atguigu.day6

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 每个10s钟做一次保存检查点的操作
    env.enableCheckpointing(10000L)
    env.setStateBackend(new FsStateBackend("file:///Users/yuanzuo/Desktop/flink-tutorial/FlinkSZ1128/checkpoint"))

    val stream = env
      .addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyKeyedProcess)

    stream.print()
    env.execute()
  }

  class MyKeyedProcess extends KeyedProcessFunction[String, SensorReading, String] {

    var listState: ListState[SensorReading] = _

    var timerTs: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      // 初始化一个列表状态变量
      listState = getRuntimeContext.getListState(
        new ListStateDescriptor[SensorReading]("list-state", Types.of[SensorReading])
      )

      timerTs = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("timer", Types.of[Long])
      )
    }

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value) // 将到来的传感器数据添加到列表状态变量
      if (timerTs.value() == 0L) {
        val ts = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timerTs.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      val list: ListBuffer[SensorReading] = ListBuffer() // 初始化一个空列表
      import scala.collection.JavaConversions._ // 必须导入
      // 将列表状态变量的数据都添加到列表中
      for (r <- listState.get()) {
        list += r
      }
      listState.clear() // gc列表状态变量

      out.collect("列表状态变量中的元素数量有 " + list.size + " 个")
      timerTs.clear()
    }
  }
}