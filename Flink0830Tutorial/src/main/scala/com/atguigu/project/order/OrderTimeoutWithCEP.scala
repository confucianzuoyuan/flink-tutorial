package com.atguigu.project.order

import com.atguigu.project.util.OrderEvent
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env
      .fromElements(
        OrderEvent("1", "create", "1"),
        OrderEvent("2", "create", "2"),
        OrderEvent("2", "pay", "3")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("begin").where(_.eventType == "create")
      .next("next").where(_.eventType == "pay")
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(orderEventStream, pattern)

    val orderTimeoutOutput = new OutputTag[String]("orderTimeout")

    val timeoutFunction = (map: scala.collection.Map[String, Iterable[OrderEvent]], timestamp: Long, out: Collector[String]) => {
      val orderStart = map("begin").head
      out.collect("订单ID为 " + orderStart.orderId + " 在5s以内没有支付")
    }

    val selectFunction = (map: scala.collection.Map[String, Iterable[OrderEvent]], out: Collector[OrderEvent]) => {}

    // 接受参数的方式是柯里化的，第一个参数：侧输出标签；第二个参数：超时事件处理函数；第三个参数：正常事件处理函数
    val timeoutOrder = patternStream.flatSelect(orderTimeoutOutput)(timeoutFunction)(selectFunction)

    timeoutOrder.getSideOutput(orderTimeoutOutput).print()

    env.execute()
  }
}