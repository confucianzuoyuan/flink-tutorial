package com.atguigu.day7

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutDetectWithoutCEP {
  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderStream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_1", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(r => r.orderId)
      .process(new OrderMatch)

    orderStream.print()

    env.execute()
  }
  class OrderMatch extends KeyedProcessFunction[String, OrderEvent, String] {

    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order-event", Types.of[OrderEvent])
    )

    override def processElement(i: OrderEvent, context: KeyedProcessFunction[String, OrderEvent, String]#Context, collector: Collector[String]): Unit = {
      if (i.eventType.equals("create")) {
        if (orderState.value() == null) {
          orderState.update(i)
          context.timerService().registerEventTimeTimer(i.eventTime + 5000L)
        } else {
          collector.collect("order id " + i.orderId + " is payed!")
        }
      } else {
        orderState.update(i)
        collector.collect("order id " + i.orderId + " is payed!")
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null && savedOrder.eventType.equals("create")) {
        out.collect("order id " + ctx.getCurrentKey + " is not payed!")
      }

      orderState.clear()
    }
  }
}