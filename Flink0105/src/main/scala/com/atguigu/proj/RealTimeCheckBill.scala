package com.atguigu.proj

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RealTimeCheckBill {

  // 订单支付事件
  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: Long)

  // 第三方机构的支付事件，weixin，zhifubao
  case class PayEvent(orderId: String,
                      eventType: String,
                      eventTime: Long)

  // 未匹配到的订单支付事件
  val unmatchedOrders = new OutputTag[String]("unmatched-orders")
  // 未匹配到的第三方支付事件
  val unmatchedPays = new OutputTag[String]("unmatched-pays")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orders = env
      .fromElements(
        OrderEvent("order_1", "pay", 2000L),
        OrderEvent("order_2", "pay", 3000L),
        OrderEvent("order_3", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)

    val pays = env
      .fromElements(
        PayEvent("order_1", "zhifubao", 5000L),
        PayEvent("order_4", "zhifubao", 6000L),
        PayEvent("order_5", "zhifubao", 7000L)
      )
      .assignAscendingTimestamps(_.eventTime)

    val processed = orders
      .connect(pays)
      .keyBy(0, 0) // 使用订单ID作为联结条件
      .process(new MatchFunction)

    processed.print() // 打印对账成功的订单
    processed.getSideOutput(unmatchedOrders).print() // 打印订单支付事件到达，第三方支付事件没有到达的订单ID
    processed.getSideOutput(unmatchedPays).print() // 打印第三方支付事件没有到达哦，订单支付事件到达的订单ID

    env.execute()
  }

  class MatchFunction extends CoProcessFunction[OrderEvent, PayEvent, String] {

    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order-state", Types.of[OrderEvent])
    )

    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("pay-state", Types.of[PayEvent])
    )

    // 用来处理来自订单支付事件流的元素
    override def processElement1(order: OrderEvent, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context, out: Collector[String]): Unit = {
      val pay = payState.value()

      if (pay != null) {
        // 同样订单ID的第三方支付事件先到了，因为pay不为空
        // 而此时订单支付事件也到了，所以对账成功了！
        // 清空payState
        payState.clear()
        out.collect("订单ID为：" + order.orderId + " 的订单对账成功！")
      } else {
        // 同样订单ID的订单支付事件先到了，因为pay为null
        // 所以先将订单支付事件存下来，等待对账，等待5s
        orderState.update(order)
        ctx.timerService().registerEventTimeTimer(order.eventTime + 5000L)
      }
    }

    // 用来处理来自第三方支付事件流的元素
    override def processElement2(pay: PayEvent, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context, out: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        out.collect("订单ID为：" + pay.orderId + " 的订单对账成功！")
      } else {
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (payState.value() != null) {
        ctx.output(unmatchedPays, "订单ID是：" + payState.value().orderId + " 对账失败，订单支付事件没来！")
        payState.clear()
      }
      if (orderState.value() != null) {
        ctx.output(unmatchedOrders, "订单ID是：" + orderState.value().orderId + " 对账失败，第三方支付事件没来！")
        orderState.clear()
      }
    }
  }
}