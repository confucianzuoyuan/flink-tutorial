package com.atguigu.project.order

import com.atguigu.project.util.OrderEvent
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {
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
      .process(new OrderMatchFunction)

    orderEventStream.print()

    env.execute()
  }

  class OrderMatchFunction extends KeyedProcessFunction[String, OrderEvent, String] {
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order", Types.of[OrderEvent])
    )

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, String]#Context, out: Collector[String]): Unit = {
      if (value.eventType == "create") {
        // pay 事件可能比 create 事件先到达
        if (orderState.value() == null) {
          orderState.update(value)
          ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5 * 1000)
        }
      } else {
        orderState.update(value)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null && savedOrder.eventType == "create") {
        out.collect("订单ID为： " + savedOrder.orderId + " 5s以内没有支付")
      }

      orderState.clear()
    }
  }
}