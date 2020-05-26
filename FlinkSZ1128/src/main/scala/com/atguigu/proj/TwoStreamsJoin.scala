package com.atguigu.proj

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamsJoin {

  // 订单支付事件
  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: Long)

  // 第三方支付事件，例如微信，支付宝
  case class PayEvent(orderId: String,
                      eventType: String,
                      eventTime: Long)
  // 用来输出没有匹配到的订单支付事件
  val unmatchedOrders = new OutputTag[String]("unmatched-orders")
  // 用来输出没有匹配到的第三方支付事件
  val unmatchedPays   = new OutputTag[String]("unmatched-pays")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orders = env
      .fromElements(
        OrderEvent("order_1", "pay", 2000L),
        OrderEvent("order_2", "pay", 5000L),
        OrderEvent("order_3", "pay", 6000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    val pays = env
      .fromElements(
        PayEvent("order_1", "weixin", 7000L),
        PayEvent("order_2", "weixin", 8000L),
        PayEvent("order_4", "weixin", 9000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    val processed = orders
      .connect(pays) // ConnectedStream
      .process(new MatchFunction)

    processed.print()

    processed.getSideOutput(unmatchedOrders).print()

    processed.getSideOutput(unmatchedPays).print()

    env.execute()
  }

  class MatchFunction extends CoProcessFunction[OrderEvent, PayEvent, String] {

    // 用来保存到来的订单支付事件
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order-state", Types.of[OrderEvent])
    )

    // 用来保存到来的第三方支付事件
    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("pay-state", Types.of[PayEvent])
    )

    // 处理订单支付流的
    override def processElement1(order: OrderEvent, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context, out: Collector[String]): Unit = {
      val pay = payState.value()

      // pay 和 order 的订单ID是一样的
      if (pay != null) {
        payState.clear()
        out.collect("订单ID为 " + order.orderId + " 的两条流对账成功！")
      } else {
        orderState.update(order)
        // 等待另一条流对应的事件，5秒钟
        ctx.timerService().registerEventTimeTimer(order.eventTime + 5000L)
      }
    }

    // 处理第三方支付流的
    override def processElement2(pay: PayEvent, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context, out: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        out.collect("订单ID为 " + pay.orderId + " 的两条流对账成功！")
      } else {
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (orderState.value() != null) {
        ctx.output(unmatchedOrders, "订单ID为 " + orderState.value().orderId + " 的两条流没有对账成功！")
        orderState.clear()
      }
      if (payState.value() != null) {
        ctx.output(unmatchedPays, "订单ID为 " + payState.value().orderId + " 的两条流没有对账成功！")
        payState.clear()
      }
    }
  }
}