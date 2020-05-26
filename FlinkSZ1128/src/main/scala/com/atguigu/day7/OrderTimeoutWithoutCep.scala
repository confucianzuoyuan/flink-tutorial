package com.atguigu.day7

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {
  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_2", "pay", 4000L)
      )
      .setParallelism(1)
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)
      .process(new OrderTimeoutFunc)

    val timeoutOutput = new OutputTag[String]("timeout")
    stream.getSideOutput(timeoutOutput).print()
    stream.print()
    env.execute()
  }

  class OrderTimeoutFunc extends KeyedProcessFunction[String, OrderEvent, String] {
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order", classOf[OrderEvent])
    )

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, String]#Context, out: Collector[String]): Unit = {
      if (value.eventType.equals("create")) {  // 到来的事件是下订单事件
        if (orderState.value() == null) { // 要判空，因为pay事件可能先到
          orderState.update(value) // 将create事件存到状态变量
          ctx.timerService().registerEventTimeTimer(value.eventTime + 5000L)
        }
      } else {
        orderState.update(value) // 将pay事件保存到状态变量
        out.collect("已经支付的订单ID是：" + value.orderId)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null && order.eventType.equals("create")) {
        ctx.output(new OutputTag[String]("timeout"), "超时订单的ID为：" + order.orderId)
      }
      orderState.clear()
    }
  }
}