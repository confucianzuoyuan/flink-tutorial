package com.atguigu.day10

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamJoin {

  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  case class PayEvent(orderId: String, eventType: String, eventTime: Long)

  val unmatchedOrders = new OutputTag[String]("unmatched-orders")
  val unmatchedPays   = new OutputTag[String]("unmatched-pays")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderStream = env
      .fromElements(
        OrderEvent("order_1", "pay", 1000L),
        OrderEvent("order_2", "pay", 2000L)
      )
      .assignAscendingTimestamps(r => r.eventTime)
      .keyBy(r => r.orderId)

    val payStream = env
      .fromElements(
        PayEvent("order_1", "weixin", 3000L),
        PayEvent("order_3", "weixin", 4000L)
      )
      .assignAscendingTimestamps(r => r.eventTime)
      .keyBy(r => r.orderId)

    val result = orderStream
      .connect(payStream)
      .process(new MatchFunction)

    result.print()

    result.getSideOutput(unmatchedOrders).print()

    result.getSideOutput(unmatchedPays).print()

    env.execute()
  }

  class MatchFunction extends CoProcessFunction[OrderEvent, PayEvent, String] {

    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order", Types.of[OrderEvent])
    )

    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("pay", Types.of[PayEvent])
    )

    override def processElement1(order: OrderEvent, context: CoProcessFunction[OrderEvent, PayEvent, String]#Context, collector: Collector[String]): Unit = {
      val pay = payState.value()

      if (pay != null) {
        payState.clear()
        collector.collect("order id: " + order.orderId + " matched success!")
      } else {
        orderState.update(order)
        context.timerService().registerEventTimeTimer(order.eventTime + 5000L)
      }
    }

    override def processElement2(pay: PayEvent, context: CoProcessFunction[OrderEvent, PayEvent, String]#Context, collector: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        collector.collect("order id: " + pay.orderId + " match success!")
      } else {
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (orderState.value() != null) {
        ctx.output(unmatchedOrders, "order id: " + orderState.value().orderId + " fail match")
        orderState.clear()
      }
      if (payState.value() != null) {
        ctx.output(unmatchedPays, "order id: " + payState.value().orderId + " fail match")
        payState.clear()
      }
    }
  }
}